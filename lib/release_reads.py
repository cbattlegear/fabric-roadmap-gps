"""Shared non-semantic public release reads; no Flask or embedding calls."""

from db import db_sqlserver as db
from lib.releases_api import ReleasesQuery, _format_release_row


def read_release_page(
    engine, query: ReleasesQuery, *,
    count_rows=db.count_recently_modified_releases,
    fetch_rows=db.get_recently_modified_releases,
) -> tuple[list[dict], int]:
    """Return public rows and total using the REST substring/filter semantics.

    Semantic ranking remains the REST handler's separate, unchanged path.
    """
    filters = dict(
        product_name=query.product_name, release_type=query.release_type,
        release_status=query.release_status,
        modified_within_days=query.modified_within_days, q=query.q,
        include_inactive=query.include_inactive,
    )
    total = count_rows(engine, **filters)
    rows = fetch_rows(
        engine, **filters, limit=query.page_size, offset=query.offset, sort=query.sort,
    )
    return [_format_release_row(row, include_description_html=False) for row in rows], total
