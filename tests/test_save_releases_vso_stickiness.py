"""Tests for ``vso_item`` stickiness in ``save_releases``.

The Fabric source intermittently drops ``VSOItem`` from a row's payload
and then includes it again on the next refresh. Without stickiness,
every flap toggles ``row_hash`` / ``last_modified`` and triggers
re-vectorization of the entire roadmap.
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


_VSO_URL = "https://dev.azure.com/powerbi/Release%20Planner/_workitems/edit/1328302/"


def _api_item(**overrides):
    base = {
        "ReleaseItemID": "00000000-0000-0000-0000-000000000010",
        "FeatureName": "Notebook integration",
        "FeatureDescription": "Description",
        "ReleaseDate": "2024-03-25",
        "ReleaseType": "Public preview",
        "ReleaseStatus": "Shipped",
        "ReleaseSemester": "2023 Wave 2",
        "ReleaseTypeValue": "578500001",
        "ReleaseStatusValue": "457530002",
        "VSOItem": _VSO_URL,
        "ProductID": "00000000-0000-0000-0000-0000000000aa",
        "ProductName": "Real-Time Intelligence",
        "isPublishExternally": "true",
    }
    base.update(overrides)
    return base


def _seed(engine, *, vso_item, last_modified=date(2024, 1, 1)):
    payload = _normalize_for_hash(_api_item(VSOItem=vso_item))
    seeded_hash = _compute_row_hash(payload)
    with session_scope(engine) as session:
        with session.begin():
            session.add(
                ReleaseItemModel(
                    release_item_id="00000000-0000-0000-0000-000000000010",
                    feature_name="Notebook integration",
                    feature_description="Description",
                    release_date=date(2024, 3, 25),
                    release_type="Public preview",
                    release_status="Shipped",
                    release_semester="2023 Wave 2",
                    release_type_value=578500001,
                    release_status_value=457530002,
                    vso_item=vso_item,
                    product_id="00000000-0000-0000-0000-0000000000aa",
                    product_name="Real-Time Intelligence",
                    is_publish_externally=True,
                    row_hash=seeded_hash,
                    last_modified=last_modified,
                    active=True,
                )
            )
    return seeded_hash


def _fetch(engine):
    with session_scope(engine) as session:
        return session.get(ReleaseItemModel, "00000000-0000-0000-0000-000000000010")


class TestVsoItemStickiness:
    def test_existing_vso_kept_when_source_drops_it(self, engine):
        seeded_hash = _seed(engine, vso_item=_VSO_URL)

        # Source omits VSOItem entirely (key missing).
        item = _api_item()
        del item["VSOItem"]
        stats = save_releases(engine, [item])

        row = _fetch(engine)
        assert row.vso_item == _VSO_URL
        assert row.row_hash == seeded_hash
        assert row.last_modified == date(2024, 1, 1)
        assert stats["unchanged"] == 1
        assert stats["updated"] == 0

    def test_existing_vso_kept_when_source_sends_null(self, engine):
        seeded_hash = _seed(engine, vso_item=_VSO_URL)

        stats = save_releases(engine, [_api_item(VSOItem=None)])

        row = _fetch(engine)
        assert row.vso_item == _VSO_URL
        assert row.row_hash == seeded_hash
        assert stats["unchanged"] == 1

    def test_existing_vso_kept_when_source_sends_blank(self, engine):
        seeded_hash = _seed(engine, vso_item=_VSO_URL)

        stats = save_releases(engine, [_api_item(VSOItem="   ")])

        row = _fetch(engine)
        assert row.vso_item == _VSO_URL
        assert row.row_hash == seeded_hash
        assert stats["unchanged"] == 1

    def test_new_vso_silently_overwrites_existing(self, engine):
        # VSOItem is excluded from the hash, so the row is "unchanged" from
        # a content-tracking perspective, but we still sync the persisted
        # value so the column reflects source reality.
        seeded_hash = _seed(engine, vso_item=_VSO_URL)
        new_url = "https://dev.azure.com/powerbi/_workitems/edit/9999999/"

        stats = save_releases(engine, [_api_item(VSOItem=new_url)])

        row = _fetch(engine)
        assert row.vso_item == new_url
        # Hash unchanged, last_modified unchanged, counted as unchanged.
        assert row.row_hash == seeded_hash
        assert row.last_modified == date(2024, 1, 1)
        assert stats["unchanged"] == 1
        assert stats["updated"] == 0

    def test_empty_source_inserts_null_for_new_row(self, engine):
        # No existing row; nothing to be sticky about.
        item = _api_item()
        del item["VSOItem"]
        stats = save_releases(engine, [item])

        row = _fetch(engine)
        assert row.vso_item is None
        assert stats["inserted"] == 1

    def test_empty_existing_takes_new_value(self, engine):
        seeded_hash = _seed(engine, vso_item=None)
        stats = save_releases(engine, [_api_item(VSOItem=_VSO_URL)])

        row = _fetch(engine)
        # Silently synced even though hash didn't change.
        assert row.vso_item == _VSO_URL
        assert row.row_hash == seeded_hash
        assert stats["unchanged"] == 1
        assert stats["updated"] == 0

    def test_releaseitem_dataclass_path_preserves_existing_vso(self, engine):
        # Regression: the production code path runs items through
        # ReleaseItem.from_dict before save_releases. Ensure stickiness
        # works there too.
        from lib.release_item import ReleaseItem

        seeded_hash = _seed(engine, vso_item=_VSO_URL)

        item_dict = _api_item()
        del item_dict["VSOItem"]
        items = [ReleaseItem.from_dict(item_dict)]
        stats = save_releases(engine, items)

        row = _fetch(engine)
        assert row.vso_item == _VSO_URL
        assert row.row_hash == seeded_hash
        assert stats["unchanged"] == 1
