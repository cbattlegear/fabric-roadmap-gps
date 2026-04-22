"""Tests for ``db.db_sqlserver.session_scope``.

Uses an in-memory SQLite engine so we exercise the real SQLAlchemy
machinery (Session lifecycle, transaction commit/rollback, attribute
detachment) without needing SQL Server / pyodbc.
"""

import pytest
import sqlalchemy
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import declarative_base

from db.db_sqlserver import session_scope


Base = declarative_base()


class _Toy(Base):
    __tablename__ = "toy"
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(eng)
    return eng


def test_session_scope_yields_session_bound_to_engine(engine):
    with session_scope(engine) as session:
        assert isinstance(session, sqlalchemy.orm.Session)
        assert session.bind is engine


def test_session_scope_closes_session_on_exit(engine):
    captured = {}
    with session_scope(engine) as session:
        captured["session"] = session
        assert session.is_active

    # ``Session.is_active`` stays True after close in modern SQLAlchemy
    # (close ends the transaction but the Session itself can be reused).
    # The reliable post-close signal is that ``in_transaction()`` is False
    # and that no autobegin transaction is currently open.
    assert captured["session"].in_transaction() is False


def test_nested_session_begin_commits_on_success(engine):
    """The nested ``with session.begin():`` pattern (used in 12 sites
    today) must still commit when the inner block exits cleanly."""
    with session_scope(engine) as session:
        with session.begin():
            session.add(_Toy(id=1, name="alpha"))

    # Open a fresh session to verify the row was actually committed.
    with session_scope(engine) as session:
        toy = session.get(_Toy, 1)
        assert toy is not None
        assert toy.name == "alpha"


def test_nested_session_begin_rolls_back_on_exception(engine):
    """An exception inside ``with session.begin():`` must roll back so
    callers don't end up with half-applied state."""
    with pytest.raises(RuntimeError, match="boom"):
        with session_scope(engine) as session:
            with session.begin():
                session.add(_Toy(id=2, name="bravo"))
                raise RuntimeError("boom")

    with session_scope(engine) as session:
        assert session.get(_Toy, 2) is None


def test_session_scope_does_not_implicitly_commit(engine):
    """``session_scope`` is lifecycle-only — it must NOT commit on its
    own. Otherwise every read-only call site would trigger a needless
    transaction flush, and the 12 ``with session.begin():`` blocks would
    have their semantics altered."""
    with session_scope(engine) as session:
        # Add a row WITHOUT wrapping in session.begin(). Since session_scope
        # doesn't commit, the row must not persist.
        session.add(_Toy(id=3, name="charlie"))

    with session_scope(engine) as session:
        assert session.get(_Toy, 3) is None


def test_session_scope_each_call_returns_new_session(engine):
    """Two consecutive calls must return different Session instances so
    state from one block can't leak into the next."""
    with session_scope(engine) as s1:
        s1_id = id(s1)
    with session_scope(engine) as s2:
        s2_id = id(s2)
    assert s1_id != s2_id


def test_expunge_all_detaches_orm_rows(engine):
    """M13 convention check: rows that were expunge_all'd while the
    session was open are flagged as detached after the session closes,
    which is exactly what callers need to safely use them outside the
    ``with`` block.
    """
    with session_scope(engine) as session:
        with session.begin():
            session.add(_Toy(id=99, name="zulu"))

    with session_scope(engine) as session:
        rows = session.query(_Toy).all()
        session.expunge_all()

    # Outside the ``with`` block the session is closed. A detached row
    # can still have its already-loaded attributes read (so callers don't
    # blow up touching ``row.name``), and ``inspect`` confirms the state.
    assert len(rows) >= 1
    state = sqlalchemy.inspect(rows[0])
    assert state.detached is True
    assert rows[0].name == "zulu"  # loaded attribute is safe to read
