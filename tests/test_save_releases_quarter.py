"""Tests for ``save_releases`` quarter-date stickiness.

When the Fabric source switched ``ReleaseDate`` from a calendar date to a
``"Q# YYYY"`` quarter token, every existing row would otherwise be
re-stamped to the quarter-end date - bumping ``last_modified`` and
``row_hash`` and triggering re-vectorization for the entire roadmap.

The save path keeps the existing ``release_date`` whenever it falls
inside the new quarter so legitimate same-quarter rows stay quiet.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine

from db.db_sqlserver import (
    Base,
    ReleaseItemModel,
    _compute_row_hash,
    _normalize_for_hash,
    save_releases,
    session_scope,
)


@pytest.fixture
def engine():
    eng = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(eng)
    return eng


def _api_item(**overrides):
    base = {
        "ReleaseItemID": "id-1",
        "FeatureName": "Feature One",
        "FeatureDescription": "Description",
        "ReleaseDate": "Q4 2024",
        "ReleaseType": "GA",
        "ReleaseStatus": "Shipped",
        "ReleaseSemester": "2024 Wave 1",
        "ReleaseTypeValue": "1",
        "ReleaseStatusValue": "2",
        "VSOItem": "VSO-1",
        "ProductID": 100,
        "ProductName": "Power BI",
        "isPublishExternally": "true",
    }
    base.update(overrides)
    return base


def _seed(engine, *, release_date, last_modified=date(2024, 1, 1)):
    # Pre-compute a row_hash that matches what save_releases will produce
    # *after* the stickiness adjustment - i.e. with the existing
    # release_date carried into the hash payload. That way an existing
    # within-quarter date round-trips as "unchanged".
    payload = _normalize_for_hash(_api_item())
    payload["ReleaseDate"] = release_date.isoformat() if release_date else None
    seeded_hash = _compute_row_hash(payload)
    with session_scope(engine) as session:
        with session.begin():
            session.add(
                ReleaseItemModel(
                    release_item_id="id-1",
                    feature_name="Feature One",
                    feature_description="Description",
                    release_date=release_date,
                    release_type="GA",
                    release_status="Shipped",
                    release_semester="2024 Wave 1",
                    release_type_value=1,
                    release_status_value=2,
                    vso_item="VSO-1",
                    product_id="100",
                    product_name="Power BI",
                    is_publish_externally=True,
                    row_hash=seeded_hash,
                    last_modified=last_modified,
                    active=True,
                )
            )
    return seeded_hash


def _fetch(engine):
    with session_scope(engine) as session:
        return session.get(ReleaseItemModel, "id-1")


class TestQuarterStickiness:
    def test_existing_date_within_quarter_is_preserved(self, engine):
        # Seed: an exact date that falls inside Q4 2024.
        existing = date(2024, 11, 7)
        seeded_hash = _seed(engine, release_date=existing)

        # Source now publishes "Q4 2024" for the same item.
        stats = save_releases(engine, [_api_item(ReleaseDate="Q4 2024")])

        row = _fetch(engine)
        # Date stayed; last_modified did not bump; counted as unchanged.
        assert row.release_date == existing
        assert row.last_modified == date(2024, 1, 1)
        assert row.row_hash == seeded_hash
        assert stats["unchanged"] == 1
        assert stats["updated"] == 0

    def test_existing_date_outside_quarter_is_overwritten_to_quarter_end(self, engine):
        # Seed: a date that does NOT fall inside Q4 2024.
        seeded_hash = _seed(engine, release_date=date(2024, 6, 30))

        stats = save_releases(engine, [_api_item(ReleaseDate="Q4 2024")])

        row = _fetch(engine)
        assert row.release_date == date(2024, 12, 31)
        assert row.last_modified == date.today()
        assert row.row_hash != seeded_hash
        assert stats["updated"] == 1
        # Re-vectorization is requested by clearing these on content change.
        assert row.release_vector is None
        assert row.blog_title is None
        assert row.blog_url is None

    def test_new_row_with_quarter_token_inserts_quarter_end(self, engine):
        stats = save_releases(engine, [_api_item(ReleaseDate="Q2 2025")])
        row = _fetch(engine)
        assert row.release_date == date(2025, 6, 30)
        assert stats["inserted"] == 1

    def test_existing_null_release_date_takes_quarter_end(self, engine):
        _seed(engine, release_date=None)
        save_releases(engine, [_api_item(ReleaseDate="Q4 2024")])
        row = _fetch(engine)
        assert row.release_date == date(2024, 12, 31)

    def test_legacy_date_string_still_overwrites(self, engine):
        _seed(engine, release_date=date(2024, 11, 7))
        save_releases(engine, [_api_item(ReleaseDate="2025-02-15")])
        row = _fetch(engine)
        assert row.release_date == date(2025, 2, 15)
