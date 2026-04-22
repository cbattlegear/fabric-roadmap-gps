"""Tests for ``lib.db_retry``: transient detection + retry decorator."""

from unittest.mock import patch

import pytest

from lib.db_retry import (
    is_transient_sql_azure_error,
    retry_on_transient_errors,
)


class TestIsTransientSqlAzureError:
    @pytest.mark.parametrize(
        "message",
        [
            "Login failed for user 'foo'",
            "A transport-level error has occurred when receiving results from the server",
            "Connection timeout expired",
            "Server is busy. Please try again later.",
            "Operation timed out",
            "The connection is broken and recovery is not possible",
            "[40613] Database 'x' on server 'y' is not currently available",
            "Cannot open database requested by the login",
            "Transaction was deadlocked on lock resources with another process",
            "Error 10928: Resource ID : 1. The request limit for the database is N",
        ],
    )
    def test_known_transient_messages_are_classified_transient(self, message):
        exc = Exception(message)
        assert is_transient_sql_azure_error(exc) is True

    @pytest.mark.parametrize(
        "message",
        [
            "Invalid column name 'foo'",
            "Permission denied on object 'release_items'",
            "Syntax error near 'SELET'",
            "Conversion failed when converting the nvarchar value 'abc'",
            "Cannot insert duplicate key in object 'dbo.release_items'",
        ],
    )
    def test_known_non_transient_messages_are_classified_non_transient(self, message):
        exc = Exception(message)
        assert is_transient_sql_azure_error(exc) is False

    def test_none_returns_false(self):
        assert is_transient_sql_azure_error(None) is False

    def test_transient_codes_match_inside_longer_messages(self):
        # 1205 = deadlock victim
        exc = RuntimeError("pyodbc.Error: ('40001', 'Transaction (Process ID 52) ... 1205')")
        assert is_transient_sql_azure_error(exc) is True


class TestRetryOnTransientErrors:
    def test_success_on_first_attempt_does_not_retry(self):
        calls = []

        @retry_on_transient_errors(max_attempts=3, initial_delay=0)
        def op():
            calls.append(1)
            return "ok"

        assert op() == "ok"
        assert calls == [1]

    def test_non_transient_raises_immediately_without_retry(self):
        calls = []

        @retry_on_transient_errors(max_attempts=5, initial_delay=0)
        def op():
            calls.append(1)
            raise ValueError("Invalid column name 'foo'")

        with pytest.raises(ValueError):
            op()
        assert len(calls) == 1

    def test_transient_then_success_returns_value(self):
        calls = []

        @retry_on_transient_errors(max_attempts=3, initial_delay=0)
        def op():
            calls.append(1)
            if len(calls) < 3:
                raise RuntimeError("Connection timeout")
            return "recovered"

        with patch("lib.db_retry.time.sleep"):
            assert op() == "recovered"
        assert len(calls) == 3

    def test_transient_exhausts_attempts_and_re_raises(self):
        calls = []

        @retry_on_transient_errors(max_attempts=3, initial_delay=0)
        def op():
            calls.append(1)
            raise RuntimeError("Server is busy")

        with patch("lib.db_retry.time.sleep"):
            with pytest.raises(RuntimeError, match="Server is busy"):
                op()
        # Total attempts == max_attempts (initial + retries)
        assert len(calls) == 3

    def test_backoff_delay_grows_and_caps(self):
        sleeps = []

        @retry_on_transient_errors(
            max_attempts=5,
            initial_delay=1.0,
            backoff=2.0,
            max_delay=4.0,
        )
        def op():
            raise RuntimeError("Server is busy")

        # Make jitter deterministic (random.random -> 0).
        with patch("lib.db_retry.time.sleep", side_effect=sleeps.append), \
             patch("lib.db_retry.random.random", return_value=0.0):
            with pytest.raises(RuntimeError):
                op()

        # 4 sleeps for 5 attempts. Delays: 1.0, 2.0, 4.0 (capped), 4.0 (capped).
        assert sleeps == [1.0, 2.0, 4.0, 4.0]

    def test_jitter_is_added_within_bounds(self):
        sleeps = []

        @retry_on_transient_errors(
            max_attempts=2,
            initial_delay=1.0,
            backoff=2.0,
            max_delay=10.0,
        )
        def op():
            raise RuntimeError("Connection timeout")

        with patch("lib.db_retry.time.sleep", side_effect=sleeps.append), \
             patch("lib.db_retry.random.random", return_value=0.5):
            with pytest.raises(RuntimeError):
                op()

        # initial_delay (1.0) + jitter (0.5 * 0.5 = 0.25) = 1.25
        assert sleeps == [1.25]

    def test_decorator_preserves_function_name_and_doc(self):
        @retry_on_transient_errors()
        def my_func():
            """My docstring."""
            return 1

        assert my_func.__name__ == "my_func"
        assert my_func.__doc__ == "My docstring."
        assert hasattr(my_func, "__wrapped__")

    def test_args_and_kwargs_passed_through(self):
        @retry_on_transient_errors()
        def add(a, b, multiplier=1):
            return (a + b) * multiplier

        assert add(2, 3, multiplier=4) == 20
