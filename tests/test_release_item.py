"""Tests for ``lib.release_item.ReleaseItem`` parsing helpers."""

import uuid
from datetime import date

import pytest

from lib.release_item import ReleaseItem


class TestParseUuid:
    def test_none_returns_none(self):
        assert ReleaseItem._parse_uuid(None) is None

    def test_empty_string_returns_none(self):
        assert ReleaseItem._parse_uuid("") is None

    def test_valid_uuid_string(self):
        s = "550e8400-e29b-41d4-a716-446655440000"
        assert ReleaseItem._parse_uuid(s) == uuid.UUID(s)

    def test_braced_uuid_string(self):
        s = "{550e8400-e29b-41d4-a716-446655440000}"
        assert ReleaseItem._parse_uuid(s) == uuid.UUID(s.strip("{}"))

    def test_uuid_instance_passes_through(self):
        u = uuid.uuid4()
        assert ReleaseItem._parse_uuid(u) is u

    def test_invalid_string_returns_none(self):
        assert ReleaseItem._parse_uuid("not-a-uuid") is None


class TestParseDate:
    def test_none_returns_none(self):
        assert ReleaseItem._parse_date(None) is None

    def test_empty_string_returns_none(self):
        assert ReleaseItem._parse_date("") is None

    def test_us_format(self):
        assert ReleaseItem._parse_date("03/15/2026") == date(2026, 3, 15)

    def test_iso_format(self):
        assert ReleaseItem._parse_date("2026-03-15") == date(2026, 3, 15)

    def test_iso_with_time(self):
        # Falls through to fromisoformat path
        assert ReleaseItem._parse_date("2026-03-15T10:30:00") == date(2026, 3, 15)

    def test_date_instance_passes_through(self):
        d = date(2026, 3, 15)
        assert ReleaseItem._parse_date(d) is d

    def test_garbage_returns_none(self):
        assert ReleaseItem._parse_date("not a date") is None


class TestParseBool:
    @pytest.mark.parametrize("value", [True, "true", "True", "1", "yes", "Y", "t"])
    def test_truthy_values(self, value):
        assert ReleaseItem._parse_bool(value) is True

    @pytest.mark.parametrize("value", [False, "false", "False", "0", "no", "N", "f"])
    def test_falsey_values(self, value):
        assert ReleaseItem._parse_bool(value) is False

    def test_none_returns_none(self):
        assert ReleaseItem._parse_bool(None) is None

    def test_unrecognized_returns_none(self):
        assert ReleaseItem._parse_bool("maybe") is None


class TestParseInt:
    def test_int_passes_through(self):
        assert ReleaseItem._parse_int(5) == 5

    def test_string_int(self):
        assert ReleaseItem._parse_int("42") == 42

    def test_none_returns_none(self):
        assert ReleaseItem._parse_int(None) is None

    def test_empty_string_returns_none(self):
        assert ReleaseItem._parse_int("") is None

    def test_garbage_returns_none(self):
        assert ReleaseItem._parse_int("abc") is None


class TestFromDictAndToDict:
    def test_round_trip_full_record(self):
        u = uuid.uuid4()
        product_id = uuid.uuid4()
        d = {
            "ReleaseItemID": str(u),
            "FeatureName": "Direct Lake",
            "ReleaseDate": "03/15/2026",
            "ReleaseType": "Public Preview",
            "ReleaseTypeValue": "1",
            "VSOItem": "AB#12345",
            "ReleaseStatus": "Released",
            "ReleaseStatusValue": "2",
            "ReleaseSemester": "2026 Wave 1",
            "ProductID": str(product_id),
            "ProductName": "Power BI",
            "isPublishExternally": "true",
            "FeatureDescription": "A new feature",
        }
        item = ReleaseItem.from_dict(d)
        assert item.release_item_id == u
        assert item.feature_name == "Direct Lake"
        assert item.release_date == date(2026, 3, 15)
        assert item.release_type_value == 1
        assert item.is_publish_externally is True
        assert item.product_id == product_id

        out = item.to_dict()
        assert out["ReleaseItemID"] == str(u)
        assert out["ReleaseDate"] == "03/15/2026"
        assert out["isPublishExternally"] == "true"

    def test_from_dict_handles_missing_keys(self):
        item = ReleaseItem.from_dict({})
        assert item.feature_name is None
        assert item.release_date is None
        assert item.release_item_id is None

    def test_to_dict_handles_none_fields(self):
        item = ReleaseItem()
        out = item.to_dict()
        assert out["ReleaseItemID"] is None
        assert out["ReleaseDate"] is None
        assert out["isPublishExternally"] is None
