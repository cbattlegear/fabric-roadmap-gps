"""Tests for ``_RELEASE_FIELDS``, ``_normalize_for_hash``, and
``_map_to_model_kwargs`` (M8 — single source of truth for release fields).

The two helpers used to maintain parallel lists of (PascalCase API name,
snake_case model attr) pairs. A renaming refactor that touched only one
of them would silently invalidate every row hash, causing every roadmap
item to re-vectorize on the next run. These tests pin the contract.
"""

from datetime import date

from db.db_sqlserver import (
    _RELEASE_FIELDS,
    _compute_row_hash,
    _map_to_model_kwargs,
    _normalize_for_hash,
)


# A fully-populated input using PascalCase keys (mirrors what the Fabric
# roadmap API returns).
_API_ITEM = {
    "ReleaseItemID": "abcd-1234",
    "FeatureName": "Feature X",
    "FeatureDescription": "Feature X description",
    "ReleaseDate": "2026-04-01",
    "ReleaseType": "GA",
    "ReleaseStatus": "Launched",
    "ReleaseSemester": "2026 H1",
    "ReleaseTypeValue": "1",
    "ReleaseStatusValue": "2",
    "VSOItem": "VSO-9999",
    "ProductID": 12345,
    "ProductName": "Fabric",
    "isPublishExternally": "true",
}


# Same content using snake_case keys (mirrors the ReleaseItem dataclass).
_MODEL_ITEM = {
    "release_item_id": "abcd-1234",
    "feature_name": "Feature X",
    "feature_description": "Feature X description",
    "release_date": date(2026, 4, 1),
    "release_type": "GA",
    "release_status": "Launched",
    "release_semester": "2026 H1",
    "release_type_value": 1,
    "release_status_value": 2,
    "vso_item": "VSO-9999",
    "product_id": "12345",
    "product_name": "Fabric",
    "is_publish_externally": True,
}


class TestReleaseFieldsRegistry:
    def test_release_item_id_is_excluded(self):
        """``release_item_id`` identifies the row but is not part of its
        content. It must never enter the hash, otherwise every primary-key
        change would force re-vectorization."""
        api_names = {f.api_name for f in _RELEASE_FIELDS}
        model_attrs = {f.model_attr for f in _RELEASE_FIELDS}
        assert "ReleaseItemID" not in api_names
        assert "release_item_id" not in model_attrs

    def test_all_expected_content_fields_present(self):
        """If a developer adds/removes a content field on
        ``ReleaseItemModel`` they MUST update ``_RELEASE_FIELDS`` too —
        this test will fail until they do."""
        expected_model_attrs = {
            "feature_name",
            "feature_description",
            "release_date",
            "release_type",
            "release_status",
            "release_semester",
            "release_type_value",
            "release_status_value",
            "vso_item",
            "product_id",
            "product_name",
            "is_publish_externally",
        }
        assert {f.model_attr for f in _RELEASE_FIELDS} == expected_model_attrs


class TestNormalizeForHash:
    def test_pascalcase_input(self):
        out = _normalize_for_hash(_API_ITEM)
        assert out == {
            "FeatureName": "Feature X",
            "FeatureDescription": "Feature X description",
            "ReleaseDate": "2026-04-01",
            "ReleaseType": "GA",
            "ReleaseStatus": "Launched",
            "ReleaseSemester": "2026 H1",
            "ReleaseTypeValue": 1,
            "ReleaseStatusValue": 2,
            "VSOItem": "VSO-9999",
            "ProductID": "12345",
            "ProductName": "Fabric",
            "isPublishExternally": True,
        }

    def test_snakecase_input_produces_same_hash(self):
        """Both helpers fall through to ``_get(item, api_name, model_attr)``
        — the underlying ``_get`` accepts whichever key is present. So
        both representations must hash identically."""
        h1 = _compute_row_hash(_normalize_for_hash(_API_ITEM))
        h2 = _compute_row_hash(_normalize_for_hash(_MODEL_ITEM))
        assert h1 == h2

    def test_none_freetext_normalizes_to_empty_string(self):
        """``feature_name`` and ``feature_description`` use ``v or ''`` in
        the hash so a NULL→"" UI cleanup doesn't churn the hash."""
        item = {**_API_ITEM, "FeatureName": None, "FeatureDescription": None}
        out = _normalize_for_hash(item)
        assert out["FeatureName"] == ""
        assert out["FeatureDescription"] == ""

    def test_missing_release_date_yields_none(self):
        item = {**_API_ITEM, "ReleaseDate": None}
        assert _normalize_for_hash(item)["ReleaseDate"] is None

    def test_release_item_id_does_not_affect_hash(self):
        """The whole point of M8: changing the row's PK must not change
        its content hash."""
        item_a = {**_API_ITEM, "ReleaseItemID": "id-aaa"}
        item_b = {**_API_ITEM, "ReleaseItemID": "id-bbb"}
        assert _compute_row_hash(_normalize_for_hash(item_a)) == _compute_row_hash(_normalize_for_hash(item_b))


class TestMapToModelKwargs:
    def test_pascalcase_input(self):
        out = _map_to_model_kwargs(_API_ITEM)
        assert out["release_item_id"] == "abcd-1234"
        assert out["feature_name"] == "Feature X"
        assert out["feature_description"] == "Feature X description"
        assert out["release_date"] == date(2026, 4, 1)
        assert out["release_type"] == "GA"
        assert out["release_type_value"] == 1
        assert out["release_status_value"] == 2
        assert out["product_id"] == "12345"
        assert out["product_name"] == "Fabric"
        assert out["is_publish_externally"] is True

    def test_missing_release_item_id_generates_uuid(self):
        item = {k: v for k, v in _API_ITEM.items() if k != "ReleaseItemID"}
        out = _map_to_model_kwargs(item)
        # Should be a valid UUID string (length 36, contains hyphens).
        assert isinstance(out["release_item_id"], str)
        assert len(out["release_item_id"]) == 36
        assert out["release_item_id"].count("-") == 4

    def test_none_freetext_preserved_as_none(self):
        """Unlike the hash, the model preserves real ``None`` for free-text
        fields so it round-trips correctly."""
        item = {**_API_ITEM, "FeatureName": None, "FeatureDescription": None}
        out = _map_to_model_kwargs(item)
        assert out["feature_name"] is None
        assert out["feature_description"] is None

    def test_invalid_int_yields_none(self):
        item = {**_API_ITEM, "ReleaseTypeValue": "not-a-number"}
        assert _map_to_model_kwargs(item)["release_type_value"] is None

    def test_no_overlapping_keys_with_hash_payload(self):
        """A fresh sanity check: every key the hash emits is PascalCase,
        every key the model emits is snake_case, so there's zero overlap.
        If this ever fails, someone introduced ambiguity that could let
        the two helpers drift."""
        hash_keys = set(_normalize_for_hash(_API_ITEM).keys())
        model_keys = set(_map_to_model_kwargs(_API_ITEM).keys())
        assert hash_keys.isdisjoint(model_keys)
