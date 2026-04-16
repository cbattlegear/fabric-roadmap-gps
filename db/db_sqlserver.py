import os
import json
import uuid
import hashlib
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Iterable, Any, Tuple, Dict
import urllib.parse
import time
import random
import logging

# ...existing code...
from sqlalchemy import (
    create_engine, Column, String, Integer, Date, DateTime, Boolean, Text, MetaData, func, select, or_, delete
)
from typing import Iterable, Any, Tuple, Dict, Optional, List
from sqlalchemy.orm import declarative_base, sessionmaker

# SQLAlchemy-specific exceptions we may inspect
from sqlalchemy.exc import DBAPIError

naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(naming_convention=naming_convention)
Base = declarative_base(metadata=metadata)
logger_name = __name__
opentelemetery_logger_name = f'{logger_name}.opentelemetry'
logger = logging.getLogger(opentelemetery_logger_name)


class ReleaseItemModel(Base):
    __tablename__ = "release_items"
    release_item_id = Column(String(36), primary_key=True)
    feature_name = Column(String(400))
    release_date = Column(Date, nullable=True)
    release_type = Column(String(100), index=True)
    release_type_value = Column(Integer)
    vso_item = Column(String(1000))
    release_status = Column(String(100), index=True)
    release_status_value = Column(Integer)
    release_semester = Column(String(200))
    product_id = Column(String(36), index=True)
    product_name = Column(String(200), index=True)
    is_publish_externally = Column(Boolean)
    feature_description = Column(Text)
    # SHA256 hex of the content fields (used to detect changes)
    row_hash = Column(String(64), nullable=False, index=True)
    # Last date row content was changed/inserted (UTC date only)
    last_modified = Column(Date, nullable=False, default=date.today)
    # Blog article references
    blog_title = Column(String(500), nullable=True)
    blog_url = Column(String(1000), nullable=True)
    # Vector embedding for semantic search (stored as TEXT in SQLAlchemy, actual SQL Server type is vector(1536))
    release_vector = Column(Text, nullable=True)
    # Whether this release is still present on the Fabric roadmap
    active = Column(Boolean, nullable=False, server_default='1', default=True)


class EmailSubscriptionModel(Base):
    __tablename__ = "email_subscriptions"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String(255), nullable=False, unique=True, index=True)
    verification_token = Column(String(64), nullable=True, index=True)
    is_verified = Column(Boolean, default=False, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    verified_at = Column(DateTime, nullable=True)
    last_email_sent = Column(DateTime, nullable=True)
    unsubscribe_token = Column(String(64), nullable=False, index=True)
    
    # Optional filters for personalized emails
    product_filter = Column(String(200), nullable=True)  # Comma-separated product names
    release_type_filter = Column(String(200), nullable=True)  # Comma-separated release types
    release_status_filter = Column(String(200), nullable=True)  # Comma-separated release statuses

    # Bounce tracking
    bounce_count = Column(Integer, nullable=False, server_default='0', default=0)
    last_bounced_at = Column(DateTime, nullable=True)

    # Email cadence: 'daily' or 'weekly'
    email_cadence = Column(String(10), nullable=False, server_default='weekly', default='weekly')


class FeatureWatchModel(Base):
    __tablename__ = "feature_watches"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    subscription_id = Column(String(36), nullable=False, index=True)
    release_item_id = Column(String(36), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_notified_hash = Column(String(64), nullable=True)


class EmailVerificationModel(Base):
    __tablename__ = "email_verifications"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String(255), nullable=False, index=True)
    token = Column(String(64), nullable=False, unique=True, index=True)
    action_type = Column(String(20), nullable=False)  # 'subscribe' or 'unsubscribe'
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    expires_at = Column(DateTime, nullable=False)
    is_used = Column(Boolean, default=False, nullable=False)
    used_at = Column(DateTime, nullable=True)
    # Optional: auto-add a feature watch after verification completes
    pending_watch_release_id = Column(String(36), nullable=True)


class EmailContentCacheModel(Base):
    __tablename__ = "email_content_cache"
    id = Column(Integer, primary_key=True, autoincrement=True)
    cache_date = Column(String(10), nullable=False, index=True)
    cache_key = Column(String(255), nullable=False)
    generated_at = Column(DateTime, nullable=False)
    content_json = Column(Text, nullable=False)


def _is_transient_sql_azure_error(exc: Exception) -> bool:
    """
    Try to detect common transient SQL Azure / network errors.
    Inspect message text and DBAPI/SQL error codes found in the exception.
    """
    if exc is None:
        return False

    # Known SQL/Azure transient error codes & phrases (string search)
    transient_codes = {
        "40197", "40501", "40613", "10928", "10929", "49918", "49919", "49920", "4060",
        "10053", "10054", "10060", "1205",  # 1205 = deadlock; sometimes transient as well
    }
    msg = str(exc).lower()

    # check presence of numeric codes
    for code in transient_codes:
        if code in msg:
            return True

    # common transient / network phrases
    transient_phrases = (
        "transport-level error",
        "a transport-level error has occurred",
        "the connection is broken",
        "connection reset by peer",
        "connection was closed by the server",
        "the server was not found or was not accessible",
        "cannot open database requested by the login",
        "login failed",
        "connection timeout",
        "timed out",
        "timeout expired",
        "operation timed out",
        "could not open connection",
        "server is busy",
        "service is currently paused",
        "service is busy",
        "deadlocked",  # fallback
    )
    for ph in transient_phrases:
        if ph in msg:
            return True

    # inspect SQLAlchemy DBAPIError .orig (if present) for underlying DBAPI messages
    try:
        if isinstance(exc, DBAPIError):
            orig = getattr(exc, "orig", None)
            if orig is not None and any(code in str(orig) for code in transient_codes):
                return True
            # connection_invalidated is often True when SQLAlchemy detects a disconnect
            if getattr(exc, "connection_invalidated", False):
                return True
    except Exception:
        pass

    return False


def retry_on_transient_errors(max_attempts: int = 5, initial_delay: float = 1.0, backoff: float = 2.0, max_delay: float = 30.0):
    """
    Decorator to retry a DB operation on transient SQL Azure / network errors.

    Usage:
      @retry_on_transient_errors()
      def init_db(...):
          ...
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 1
            delay = float(initial_delay)
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if not _is_transient_sql_azure_error(exc):
                        logger.exception("Non-transient DB error encountered, not retrying.")
                        raise
                    if attempt >= max_attempts:
                        logger.exception("Transient DB error and max attempts reached (%d).", max_attempts)
                        raise
                    # transient and we will retry
                    logger.warning("Transient DB error detected; attempt %d/%d will retry after %.2fs: %s",
                                   attempt, max_attempts, delay, exc)
                    # jitter
                    sleep_time = delay + (random.random() * 0.5)
                    time.sleep(sleep_time)
                    delay = min(delay * backoff, max_delay)
                    attempt += 1
        # copy some identity
        wrapper.__name__ = getattr(func, "__name__", "wrapped")
        wrapper.__doc__ = getattr(func, "__doc__", "")
        return wrapper
    return decorator


def make_engine(conn_str: str | None = None):
    """
    Build a SQLAlchemy engine. Prefer to pass a DSN or set SQLSERVER_CONN env var.
    Example env value:
      mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+18+for+SQL+Server

    This function enables pool_pre_ping and a pool_recycle so connections are checked before use,
    which helps when Azure closes idle connections.
    """
    conn = conn_str or os.environ.get("SQLSERVER_CONN") or os.environ.get("DATABASE_URL")
    if not conn:
        raise ValueError("Set SQLSERVER_CONN (or pass conn_str). Example: mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server")
    params = urllib.parse.quote_plus(conn)
    sanitized_conn = "mssql+pyodbc:///?odbc_connect=%s" % params
    logger.debug("SQLAlchemy connection string: %s", sanitized_conn)
    # fast_executemany helps bulk inserts with pyodbc; pool_pre_ping detects dead connections
    # pool_recycle ensures connections are periodically refreshed
    return create_engine(sanitized_conn, fast_executemany=True, future=True, pool_pre_ping=True, pool_recycle=3600)


@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=30.0)
def init_db(engine):
    Base.metadata.create_all(engine)


def _get(obj: Any, *keys):
    for k in keys:
        if isinstance(obj, dict) and k in obj:
            return obj[k]
        if hasattr(obj, k):
            return getattr(obj, k)
    return None


def _to_date(v):
    if not v:
        return None
    if isinstance(v, date):
        return v
    s = str(v).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return None


def _to_int(v):
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def _to_bool(v):
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "t"):
        return True
    if s in ("0", "false", "no", "n", "f"):
        return False
    return None


def _compute_row_hash(payload: Dict[str, Any]) -> str:
    # Stable JSON serialization
    j = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()


def _normalize_for_hash(item: Any) -> Dict[str, Any]:
    """
    Build a stable dict of fields that indicate content for the row.
    Excludes last_modified and row_hash.
    """
    rd = _to_date(_get(item, "ReleaseDate", "release_date"))
    return {
        "FeatureName": _get(item, "FeatureName", "feature_name") or "",
        "ReleaseDate": rd.isoformat() if rd else None,
        "ReleaseType": _get(item, "ReleaseType", "release_type"),
        "ReleaseTypeValue": _to_int(_get(item, "ReleaseTypeValue", "release_type_value")),
        "VSOItem": _get(item, "VSOItem", "vso_item"),
        "ReleaseStatus": _get(item, "ReleaseStatus", "release_status"),
        "ReleaseStatusValue": _to_int(_get(item, "ReleaseStatusValue", "release_status_value")),
        "ReleaseSemester": _get(item, "ReleaseSemester", "release_semester"),
        "ProductID": str(_get(item, "ProductID", "product_id")) if _get(item, "ProductID", "product_id") else None,
        "ProductName": _get(item, "ProductName", "product_name"),
        "isPublishExternally": _to_bool(_get(item, "isPublishExternally", "is_publish_externally")),
        "FeatureDescription": _get(item, "FeatureDescription", "feature_description") or "",
    }


def _map_to_model_kwargs(item: Any) -> Dict[str, Any]:
    rd = _to_date(_get(item, "ReleaseDate", "release_date"))
    return {
        "release_item_id": str(_get(item, "ReleaseItemID", "release_item_id") or uuid.uuid4()),
        "feature_name": _get(item, "FeatureName", "feature_name"),
        "release_date": rd,
        "release_type": _get(item, "ReleaseType", "release_type"),
        "release_type_value": _to_int(_get(item, "ReleaseTypeValue", "release_type_value")),
        "vso_item": _get(item, "VSOItem", "vso_item"),
        "release_status": _get(item, "ReleaseStatus", "release_status"),
        "release_status_value": _to_int(_get(item, "ReleaseStatusValue", "release_status_value")),
        "release_semester": _get(item, "ReleaseSemester", "release_semester"),
        "product_id": str(_get(item, "ProductID", "product_id")) if _get(item, "ProductID", "product_id") else None,
        "product_name": _get(item, "ProductName", "product_name"),
        "is_publish_externally": _to_bool(_get(item, "isPublishExternally", "is_publish_externally")),
        "feature_description": _get(item, "FeatureDescription", "feature_description"),
    }


@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def save_releases(engine, items: Iterable[Any]) -> Dict[str, int]:
    """
    Upsert a list of releases into SQL Server.
    Only updates last_modified when content changes (detected via SHA256 row_hash).
    Returns counts: {'inserted': n, 'updated': m, 'unchanged': k, 'changed_ids': [...]}
    This function is retried on transient SQL/Azure connection errors.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)
    inserted = updated = unchanged = 0
    changed_ids = []

    with SessionLocal() as session:
        with session.begin():
            for item in items:
                row_values = _map_to_model_kwargs(item)
                content_payload = _normalize_for_hash(item)
                new_hash = _compute_row_hash(content_payload)
                pk = row_values["release_item_id"]

                existing = session.get(ReleaseItemModel, pk)
                # use date only to avoid time-related sorting issues
                now = date.today()

                if existing is None:
                    # insert
                    model = ReleaseItemModel(
                        **row_values,
                        row_hash=new_hash,
                        last_modified=now,
                        active=True,
                    )
                    session.add(model)
                    inserted += 1
                    changed_ids.append(str(pk))
                else:
                    # Re-activate if previously removed from roadmap
                    if not existing.active:
                        existing.active = True
                        existing.last_modified = now
                        changed_ids.append(str(pk))

                    if (existing.row_hash or "") == new_hash:
                        unchanged += 1
                    else:
                        # update only when changed
                        for k, v in row_values.items():
                            setattr(existing, k, v)
                        existing.row_hash = new_hash
                        existing.last_modified = now
                        # NULL out release_vector and blog info when content changes
                        # This triggers re-vectorization and re-matching
                        existing.release_vector = None
                        existing.blog_title = None
                        existing.blog_url = None
                        updated += 1
                        changed_ids.append(str(pk))

    return {"inserted": inserted, "updated": updated, "unchanged": unchanged, "changed_ids": changed_ids}


@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def deactivate_missing_releases(engine, current_ids: set) -> Dict[str, int]:
    """Mark releases not in *current_ids* as inactive.

    First-run guard: if no rows are already inactive (i.e. the ``active``
    column was just added), deactivations do **not** update ``last_modified``
    so the backlog of removals won't pollute change-tracking or email digests.
    On subsequent runs the ``last_modified`` date is updated so that the
    removal surfaces through normal feeds / notifications.

    Returns counts: ``{'deactivated': n, 'already_inactive': m, 'deactivated_ids': [...]}``.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)
    deactivated = already_inactive = 0
    deactivated_ids = []

    with SessionLocal() as session:
        with session.begin():
            # Determine if this is the first run by checking for any inactive rows
            has_inactive = session.scalar(
                select(func.count()).select_from(ReleaseItemModel)
                .where(ReleaseItemModel.active == False)  # noqa: E712
            ) or 0
            is_first_run = has_inactive == 0

            now = date.today()

            # Find all currently-active rows whose IDs are not in the fetch
            active_rows = session.scalars(
                select(ReleaseItemModel)
                .where(ReleaseItemModel.active == True)  # noqa: E712
            ).all()

            for row in active_rows:
                if row.release_item_id not in current_ids:
                    row.active = False
                    if not is_first_run:
                        row.last_modified = now
                    deactivated += 1
                    deactivated_ids.append(str(row.release_item_id))

            # Count rows that were already inactive (informational)
            already_inactive = session.scalar(
                select(func.count()).select_from(ReleaseItemModel)
                .where(
                    ReleaseItemModel.active == False,  # noqa: E712
                    ReleaseItemModel.release_item_id.notin_(current_ids) if current_ids else True,
                )
            ) or 0
            # Subtract what we just deactivated (those weren't counted before commit)
            already_inactive = max(0, already_inactive - deactivated)

    logger.info("Deactivation stats: deactivated=%d, already_inactive=%d, first_run=%s",
                deactivated, already_inactive, is_first_run)
    return {"deactivated": deactivated, "already_inactive": already_inactive, "deactivated_ids": deactivated_ids}


VALID_SORT_OPTIONS = ("last_modified", "release_date")

@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def get_recently_modified_releases(
    engine,
    limit: Optional[int] = None,
    product_name: Optional[str] = None,
    release_type: Optional[str] = None,
    release_status: Optional[str] = None,
    modified_within_days: Optional[int] = None,
    q: Optional[str] = None,
    offset: Optional[int] = None,
    sort: Optional[str] = None,
    include_inactive: bool = False,
) -> List[ReleaseItemModel]:
    """
    Return releases sorted by *sort* column (desc), filtered & paginated.
    ``sort`` must be one of ``VALID_SORT_OPTIONS`` (default ``"last_modified"``).
    Only active releases are returned unless *include_inactive* is True.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)

    stmt = select(ReleaseItemModel)
    filters = []
    if not include_inactive:
        filters.append(ReleaseItemModel.active == True)  # noqa: E712
    if product_name is not None:
        filters.append(ReleaseItemModel.product_name == product_name)
    if release_type is not None:
        filters.append(ReleaseItemModel.release_type == release_type)
    if release_status is not None:
        filters.append(ReleaseItemModel.release_status == release_status)
    if modified_within_days is not None:
        from datetime import datetime as _dt
        modified_since = _dt.utcnow() - timedelta(days=modified_within_days)
        filters.append(ReleaseItemModel.last_modified >= modified_since.date())
    if q:
        q_trimmed = str(q).strip()
        if q_trimmed:
            pattern = f"%{q_trimmed}%"
            filters.append(
                or_(
                    ReleaseItemModel.feature_name.ilike(pattern),
                    ReleaseItemModel.feature_description.ilike(pattern),
                    ReleaseItemModel.product_name.ilike(pattern),
                )
            )
    if filters:
        stmt = stmt.where(*filters)

    if sort == "release_date":
        stmt = stmt.order_by(ReleaseItemModel.release_date.desc(), ReleaseItemModel.last_modified.desc(), ReleaseItemModel.release_item_id.desc())
    else:
        stmt = stmt.order_by(ReleaseItemModel.last_modified.desc(), ReleaseItemModel.release_date.desc(), ReleaseItemModel.release_item_id.desc())

    if offset is not None:
        stmt = stmt.offset(offset)
    if limit is not None:
        stmt = stmt.limit(limit)
    with SessionLocal() as session:
        rows = session.scalars(stmt).all()
    return rows

@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def count_recently_modified_releases(
    engine,
    product_name: Optional[str] = None,
    release_type: Optional[str] = None,
    release_status: Optional[str] = None,
    modified_within_days: Optional[int] = None,
    q: Optional[str] = None,
    include_inactive: bool = False,
) -> int:
    """Return total count of releases matching filters/search."""
    SessionLocal = sessionmaker(bind=engine, future=True)
    stmt = select(func.count()).select_from(ReleaseItemModel)
    filters = []
    if not include_inactive:
        filters.append(ReleaseItemModel.active == True)  # noqa: E712
    if product_name is not None:
        filters.append(ReleaseItemModel.product_name == product_name)
    if release_type is not None:
        filters.append(ReleaseItemModel.release_type == release_type)
    if release_status is not None:
        filters.append(ReleaseItemModel.release_status == release_status)
    if modified_within_days is not None:
        from datetime import datetime as _dt
        modified_since = _dt.utcnow() - timedelta(days=modified_within_days)
        filters.append(ReleaseItemModel.last_modified >= modified_since.date())
    if q:
        q_trimmed = str(q).strip()
        if q_trimmed:
            pattern = f"%{q_trimmed}%"
            filters.append(
                or_(
                    ReleaseItemModel.feature_name.ilike(pattern),
                    ReleaseItemModel.feature_description.ilike(pattern),
                    ReleaseItemModel.product_name.ilike(pattern),
                )
            )
    if filters:
        stmt = stmt.where(*filters)
    with SessionLocal() as session:
        total = session.scalar(stmt) or 0
    return int(total)


@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def get_distinct_values(engine, column_name: str, limit: Optional[int] = None) -> List[str]:
    """Return distinct non-empty string values of a column (normalized).

    Normalization groups values case-insensitively and trims whitespace.
    Returns a representative original value per group (MIN for stable display).
    """
    SessionLocal = sessionmaker(bind=engine, future=True)

    # Validate requested column exists on the model
    if not hasattr(ReleaseItemModel, column_name):
        raise ValueError(f"Unknown column: {column_name}")
    col = getattr(ReleaseItemModel, column_name)

    # Build normalized expression: LOWER(LTRIM(RTRIM(col)))
    norm = func.lower(func.ltrim(func.rtrim(col)))
    representative = func.min(col)

    stmt = (
        select(representative)
        .where(col.is_not(None))
        .where(func.ltrim(func.rtrim(col)) != "")
        .group_by(norm)
        .order_by(representative.asc())
    )
    if limit is not None:
        stmt = stmt.limit(limit)

    with SessionLocal() as session:
        values = session.scalars(stmt).all()

    # Coerce to trimmed strings and drop empties
    out: List[str] = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out.append(s)
    return out


@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def get_changelog_with_changes(
    engine,
    days: int = 30,
    include_inactive: bool = True,
    product_name: Optional[str] = None,
    release_type: Optional[str] = None,
    release_status: Optional[str] = None,
) -> List[Dict]:
    """Return changelog items with changed-column annotations in a single temporal-table query.

    Uses the same LAG-based diff logic as GetReleaseItemHistoryById but runs
    across ALL items modified within the window, returning one row per item
    with its most-recent change summary.
    """
    filters = []
    params = [days]
    if not include_inactive:
        filters.append("AND active = 1")
    if product_name:
        filters.append("AND product_name = ?")
        params.append(product_name)
    if release_type:
        filters.append("AND release_type = ?")
        params.append(release_type)
    if release_status:
        filters.append("AND release_status = ?")
        params.append(release_status)
    filter_clause = " ".join(filters)

    sql = f"""
    WITH Hist AS (
        SELECT
            release_item_id, release_date, release_type, release_status,
            feature_description, feature_name, product_name, last_modified, active,
            release_semester, vso_item, row_hash,
            ROW_NUMBER() OVER (PARTITION BY release_item_id ORDER BY ValidFrom) AS VersionNum
        FROM dbo.release_items FOR SYSTEM_TIME ALL
        WHERE release_item_id IN (
            SELECT release_item_id FROM release_items
            WHERE last_modified >= DATEADD(day, -?, CAST(GETUTCDATE() AS DATE))
            {filter_clause}
        )
    ),
    Latest AS (
        -- Find the first temporal version where last_modified matches the current value.
        -- That's the content-change version; later versions are post-processing
        -- (vectorization, blog matching) that don't touch tracked content fields.
        SELECT h.release_item_id, MIN(h.VersionNum) AS ContentVer
        FROM Hist h
        INNER JOIN release_items ri
            ON h.release_item_id = ri.release_item_id
           AND h.last_modified = ri.last_modified
        GROUP BY h.release_item_id
    ),
    Diffs AS (
        SELECT h.*,
            LAG(release_date)        OVER (PARTITION BY h.release_item_id ORDER BY h.VersionNum) AS p_release_date,
            LAG(release_type)        OVER (PARTITION BY h.release_item_id ORDER BY h.VersionNum) AS p_release_type,
            LAG(release_status)      OVER (PARTITION BY h.release_item_id ORDER BY h.VersionNum) AS p_release_status,
            LAG(feature_description) OVER (PARTITION BY h.release_item_id ORDER BY h.VersionNum) AS p_feature_description,
            LAG(feature_name)        OVER (PARTITION BY h.release_item_id ORDER BY h.VersionNum) AS p_feature_name,
            LAG(product_name)        OVER (PARTITION BY h.release_item_id ORDER BY h.VersionNum) AS p_product_name,
            LAG(release_semester)    OVER (PARTITION BY h.release_item_id ORDER BY h.VersionNum) AS p_release_semester,
            LAG(active)              OVER (PARTITION BY h.release_item_id ORDER BY h.VersionNum) AS p_active,
            LAG(row_hash)            OVER (PARTITION BY h.release_item_id ORDER BY h.VersionNum) AS p_row_hash
        FROM Hist h
    )
    SELECT
        d.release_item_id,
        d.feature_name,
        d.product_name,
        d.release_type,
        d.release_status,
        d.release_date,
        d.last_modified,
        d.active,
        ChangedColumns = CASE
            WHEN d.VersionNum = 1 THEN 'Added to roadmap'
            ELSE COALESCE(
                (SELECT STRING_AGG(v.ColName, ',') WITHIN GROUP (ORDER BY v.ColOrder)
                FROM (
                    SELECT 1 AS ColOrder,
                           CASE WHEN (d.release_date <> d.p_release_date)
                                 OR (d.release_date IS NULL AND d.p_release_date IS NOT NULL)
                                 OR (d.release_date IS NOT NULL AND d.p_release_date IS NULL)
                                THEN CONCAT('Release Date ',
                                            COALESCE(CONVERT(varchar(30), d.p_release_date, 23), '(none)'),
                                            ' -> ',
                                            COALESCE(CONVERT(varchar(30), d.release_date, 23), '(none)'))
                           END AS ColName
                    UNION ALL
                    SELECT 2,
                           CASE WHEN (d.release_type <> d.p_release_type)
                                 OR (d.release_type IS NULL AND d.p_release_type IS NOT NULL)
                                 OR (d.release_type IS NOT NULL AND d.p_release_type IS NULL)
                                THEN CONCAT('Release Type ',
                                            COALESCE(d.p_release_type, '(none)'),
                                            ' -> ',
                                            COALESCE(d.release_type, '(none)'))
                           END
                    UNION ALL
                    SELECT 3,
                           CASE WHEN (d.release_status <> d.p_release_status)
                                 OR (d.release_status IS NULL AND d.p_release_status IS NOT NULL)
                                 OR (d.release_status IS NOT NULL AND d.p_release_status IS NULL)
                                THEN CONCAT('Release Status ',
                                            COALESCE(d.p_release_status, '(none)'),
                                            ' -> ',
                                            COALESCE(d.release_status, '(none)'))
                           END
                    UNION ALL
                    SELECT 4,
                           CASE WHEN (d.feature_description <> d.p_feature_description)
                                 OR (d.feature_description IS NULL AND d.p_feature_description IS NOT NULL)
                                 OR (d.feature_description IS NOT NULL AND d.p_feature_description IS NULL)
                                THEN 'Description updated'
                           END
                    UNION ALL
                    SELECT 5,
                           CASE WHEN (d.active <> d.p_active)
                                 OR (d.active IS NULL AND d.p_active IS NOT NULL)
                                 OR (d.active IS NOT NULL AND d.p_active IS NULL)
                                THEN CASE
                                    WHEN d.active = 0 THEN 'Removed from Roadmap'
                                    WHEN d.active = 1 THEN 'Restored to Roadmap'
                                    ELSE 'Active status changed'
                                END
                           END
                    UNION ALL
                    SELECT 6,
                           CASE WHEN (d.feature_name <> d.p_feature_name)
                                 OR (d.feature_name IS NULL AND d.p_feature_name IS NOT NULL)
                                 OR (d.feature_name IS NOT NULL AND d.p_feature_name IS NULL)
                                THEN CONCAT('Name ',
                                            COALESCE(d.p_feature_name, '(none)'),
                                            ' -> ',
                                            COALESCE(d.feature_name, '(none)'))
                           END
                    UNION ALL
                    SELECT 7,
                           CASE WHEN (d.product_name <> d.p_product_name)
                                 OR (d.product_name IS NULL AND d.p_product_name IS NOT NULL)
                                 OR (d.product_name IS NOT NULL AND d.p_product_name IS NULL)
                                THEN CONCAT('Workload ',
                                            COALESCE(d.p_product_name, '(none)'),
                                            ' -> ',
                                            COALESCE(d.product_name, '(none)'))
                           END
                    UNION ALL
                    SELECT 8,
                           CASE WHEN (d.release_semester <> d.p_release_semester)
                                 OR (d.release_semester IS NULL AND d.p_release_semester IS NOT NULL)
                                 OR (d.release_semester IS NOT NULL AND d.p_release_semester IS NULL)
                                THEN CONCAT('Semester ',
                                            COALESCE(d.p_release_semester, '(none)'),
                                            ' -> ',
                                            COALESCE(d.release_semester, '(none)'))
                           END
                ) v
                WHERE v.ColName IS NOT NULL),
                -- Fallback: hash changed but no tracked column diff detected
                'Updated'
            )
        END
    FROM Diffs d
    INNER JOIN Latest l ON d.release_item_id = l.release_item_id AND d.VersionNum = l.ContentVer
    ORDER BY d.last_modified DESC, d.product_name, d.feature_name
    """

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description]
        results = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            changed_raw = row_dict.pop("ChangedColumns", "") or ""
            changed_list = [c.strip() for c in changed_raw.split(',') if c and c.strip()]
            results.append({
                "release_item_id": row_dict["release_item_id"],
                "feature_name": row_dict["feature_name"],
                "product_name": row_dict["product_name"],
                "release_type": row_dict["release_type"],
                "release_status": row_dict["release_status"],
                "release_date": row_dict.get("release_date"),
                "last_modified": row_dict.get("last_modified"),
                "active": row_dict.get("active"),
                "changed_columns": changed_list,
            })
        return results
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()


def generate_secure_token() -> str:
    """Generate a secure random token for email verification/unsubscribe"""
    return hashlib.sha256(f"{uuid.uuid4()}{time.time()}{random.random()}".encode()).hexdigest()


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def create_email_subscription(engine, email: str, filters: Optional[Dict[str, str]] = None, cadence: str = 'weekly') -> Tuple[str, str]:
    """Create an email subscription with verification token.

    Returns tuple of (subscription_id, verification_token).
    An empty verification_token means the user is already verified.
    """
    if cadence not in ('daily', 'weekly'):
        cadence = 'weekly'
    SessionLocal = sessionmaker(bind=engine, future=True)
    
    with SessionLocal() as session:
        # Check if subscription already exists
        existing = session.scalar(
            select(EmailSubscriptionModel).where(EmailSubscriptionModel.email == email)
        )
        
        if existing and existing.is_verified:
            # Already subscribed and verified
            return existing.id, ""
        
        verification_token = generate_secure_token()
        unsubscribe_token = generate_secure_token()
        
        if existing:
            # Update existing unverified subscription
            existing.verification_token = verification_token
            existing.unsubscribe_token = unsubscribe_token
            existing.created_at = datetime.utcnow()
            existing.is_active = True
            existing.email_cadence = cadence
            if filters:
                existing.product_filter = filters.get('products', '')
                existing.release_type_filter = filters.get('types', '')
                existing.release_status_filter = filters.get('statuses', '')
            subscription_id = existing.id
        else:
            # Create new subscription
            subscription = EmailSubscriptionModel(
                email=email,
                verification_token=verification_token,
                unsubscribe_token=unsubscribe_token,
                email_cadence=cadence,
                product_filter=filters.get('products', '') if filters else '',
                release_type_filter=filters.get('types', '') if filters else '',
                release_status_filter=filters.get('statuses', '') if filters else ''
            )
            session.add(subscription)
            session.flush()
            subscription_id = subscription.id
        
        # Create verification record
        verification = EmailVerificationModel(
            email=email,
            token=verification_token,
            action_type='subscribe',
            expires_at=datetime.utcnow() + timedelta(hours=24),
        )
        session.add(verification)
        session.commit()
        
        return subscription_id, verification_token


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def create_watch_verification(engine, email: str, release_item_id: str) -> Tuple[Optional[str], str]:
    """Create a verification record for adding a feature watch.

    If the email is already a verified subscriber, a watch-specific
    verification record is created. If the email is not subscribed at all,
    a new subscription is created first (unverified).

    Returns (subscription_id, verification_token). The watch is only added
    when the user verifies via the token.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)

    with SessionLocal() as session:
        existing = session.scalar(
            select(EmailSubscriptionModel).where(EmailSubscriptionModel.email == email)
        )

        if existing and existing.is_verified:
            subscription_id = existing.id
        elif existing:
            # Re-use existing unverified subscription
            existing.created_at = datetime.utcnow()
            existing.is_active = True
            subscription_id = existing.id
        else:
            # Create a new subscription (will be verified when token is used)
            sub = EmailSubscriptionModel(
                email=email,
                verification_token=generate_secure_token(),
                unsubscribe_token=generate_secure_token(),
            )
            session.add(sub)
            session.flush()
            subscription_id = sub.id

        verification_token = generate_secure_token()
        verification = EmailVerificationModel(
            email=email,
            token=verification_token,
            action_type='subscribe',
            expires_at=datetime.utcnow() + timedelta(hours=24),
            pending_watch_release_id=release_item_id,
        )
        session.add(verification)
        session.commit()

    return subscription_id, verification_token


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def verify_email_subscription(engine, token: str) -> bool:
    """Verify an email subscription using the verification token.

    If the verification record has a pending_watch_release_id, the watch
    is automatically added after successful verification.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)
    
    with SessionLocal() as session:
        # Find verification record
        verification = session.scalar(
            select(EmailVerificationModel)
            .where(EmailVerificationModel.token == token)
            .where(EmailVerificationModel.action_type == 'subscribe')
            .where(EmailVerificationModel.is_used == False)
            .where(EmailVerificationModel.expires_at > datetime.utcnow())
        )
        
        if not verification:
            return False
        
        # Find and update subscription
        subscription = session.scalar(
            select(EmailSubscriptionModel)
            .where(EmailSubscriptionModel.email == verification.email)
        )
        
        if not subscription:
            return False
        
        # Mark as verified
        subscription.is_verified = True
        subscription.verified_at = datetime.utcnow()
        subscription.verification_token = None
        
        # Mark verification as used
        verification.is_used = True
        verification.used_at = datetime.utcnow()
        
        pending_release_id = verification.pending_watch_release_id
        subscription_id = subscription.id
        
        session.commit()

    # Add pending watch outside the verification transaction
    if pending_release_id:
        add_feature_watch(engine, subscription_id, pending_release_id)

    return True


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def unsubscribe_email(engine, token: str) -> Optional[str]:
    """Unsubscribe an email using the unsubscribe token.

    Returns the email address that was removed, or None if the token was invalid.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)
    
    with SessionLocal() as session:
        subscription = session.scalar(
            select(EmailSubscriptionModel)
            .where(EmailSubscriptionModel.unsubscribe_token == token)
        )
        
        if not subscription:
            return None
        
        email = subscription.email
        stmt = delete(EmailSubscriptionModel).where(EmailSubscriptionModel.unsubscribe_token == token)
        session.execute(stmt)
        session.commit()
        return email


BOUNCE_DEACTIVATION_THRESHOLD = 3

@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def record_bounce(engine, email: str) -> Optional[Dict[str, Any]]:
    """Record a bounce for a subscriber. Deactivates after threshold.

    Returns a dict with the outcome, or None if no matching subscriber.
    Only counts one bounce per calendar day to prevent rapid deactivation.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)
    now = datetime.utcnow()

    with SessionLocal() as session:
        with session.begin():
            sub = session.scalar(
                select(EmailSubscriptionModel)
                .where(EmailSubscriptionModel.email == email)
                .where(EmailSubscriptionModel.is_active == True)  # noqa: E712
            )
            if not sub:
                return None

            # Rate-limit: one bounce count per calendar day
            if sub.last_bounced_at and sub.last_bounced_at.date() == now.date():
                return {"action": "skipped", "reason": "already_bounced_today",
                        "bounce_count": sub.bounce_count}

            sub.bounce_count = (sub.bounce_count or 0) + 1
            sub.last_bounced_at = now

            if sub.bounce_count >= BOUNCE_DEACTIVATION_THRESHOLD:
                sub.is_active = False
                logger.warning("Deactivated subscriber %s after %d bounces",
                               email, sub.bounce_count)
                return {"action": "deactivated", "bounce_count": sub.bounce_count}

            return {"action": "recorded", "bounce_count": sub.bounce_count}


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def get_active_subscriptions(engine) -> List[EmailSubscriptionModel]:
    """Get all active and verified email subscriptions"""
    SessionLocal = sessionmaker(bind=engine, future=True)
    
    with SessionLocal() as session:
        subscriptions = session.scalars(
            select(EmailSubscriptionModel)
            .where(EmailSubscriptionModel.is_active == True)
            .where(EmailSubscriptionModel.is_verified == True)
        ).all()
        
        # Detach from session to avoid lazy loading issues
        session.expunge_all()
        return list(subscriptions)
    
@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def get_unsent_active_subscriptions(engine, time_frame: int, cadence: Optional[str] = None) -> List[EmailSubscriptionModel]:
    """Get active, verified subscriptions whose last_email_sent is older than time_frame days.

    If *cadence* is provided, only subscriptions matching that cadence are returned.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        query = (
            select(EmailSubscriptionModel)
            .where(EmailSubscriptionModel.is_active == True)
            .where(EmailSubscriptionModel.is_verified == True)
            .where(
                or_(
                    EmailSubscriptionModel.last_email_sent == None,
                    EmailSubscriptionModel.last_email_sent <= datetime.utcnow() - timedelta(days=time_frame)
                )
            )
        )
        if cadence:
            query = query.where(EmailSubscriptionModel.email_cadence == cadence)
        query = query.limit(1000)
        subscriptions = session.scalars(query).all()

        session.expunge_all()
        return list(subscriptions)


CADENCE_INTERVALS = {
    'daily': 1,
    'weekly': 7,
}


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def get_digest_eligible_subscriptions(engine) -> List[EmailSubscriptionModel]:
    """Get all subscriptions eligible for a digest email based on their cadence.

    Weekly subscribers are only eligible on Mondays (UTC).
    """
    results = []
    for cadence, days in CADENCE_INTERVALS.items():
        if cadence == 'weekly' and datetime.utcnow().weekday() != 0:
            continue
        batch = get_unsent_active_subscriptions(engine, time_frame=days, cadence=cadence)
        results.extend(batch)
    return results


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def get_release_item_by_id(engine, release_item_id: str) -> Optional[ReleaseItemModel]:
    """Return a ReleaseItemModel by primary key, or None if not found."""
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        item = session.get(ReleaseItemModel, release_item_id)
        if item:
            session.expunge(item)
        return item


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def add_feature_watch(engine, subscription_id: str, release_item_id: str) -> bool:
    """Add a feature watch. Returns True if created, False if already exists."""
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        with session.begin():
            existing = session.scalar(
                select(FeatureWatchModel).where(
                    FeatureWatchModel.subscription_id == subscription_id,
                    FeatureWatchModel.release_item_id == release_item_id,
                )
            )
            if existing:
                return False
            # Stamp the current row_hash so the subscriber isn't immediately
            # alerted for a feature that hasn't changed since they subscribed.
            current_hash = session.scalar(
                select(ReleaseItemModel.row_hash).where(
                    ReleaseItemModel.release_item_id == release_item_id
                )
            )
            session.add(FeatureWatchModel(
                subscription_id=subscription_id,
                release_item_id=release_item_id,
                last_notified_hash=current_hash,
            ))
    return True


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def remove_feature_watch(engine, subscription_id: str, release_item_id: str) -> bool:
    """Remove a feature watch. Returns True if deleted, False if not found."""
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        with session.begin():
            result = session.execute(
                delete(FeatureWatchModel).where(
                    FeatureWatchModel.subscription_id == subscription_id,
                    FeatureWatchModel.release_item_id == release_item_id,
                )
            )
    return (result.rowcount or 0) > 0


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def get_watched_changes(engine, subscription_id: str) -> List[Dict[str, Any]]:
    """Return watched releases whose row_hash differs from last_notified_hash."""
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        rows = session.execute(
            select(FeatureWatchModel, ReleaseItemModel)
            .join(ReleaseItemModel, FeatureWatchModel.release_item_id == ReleaseItemModel.release_item_id)
            .where(FeatureWatchModel.subscription_id == subscription_id)
            .where(
                or_(
                    FeatureWatchModel.last_notified_hash == None,
                    FeatureWatchModel.last_notified_hash != ReleaseItemModel.row_hash,
                )
            )
        ).all()
        results = []
        for watch, release in rows:
            results.append({
                'watch_id': watch.id,
                'release_item_id': release.release_item_id,
                'feature_name': release.feature_name,
                'product_name': release.product_name,
                'release_type': release.release_type,
                'release_status': release.release_status,
                'feature_description': release.feature_description,
                'last_modified': release.last_modified.isoformat() if release.last_modified else None,
                'active': release.active,
                'current_hash': release.row_hash,
            })
        return results


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def update_watch_hashes(engine, hash_updates: List[Tuple[str, str]]):
    """Update last_notified_hash for a list of (watch_id, new_hash) tuples."""
    if not hash_updates:
        return
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        with session.begin():
            for watch_id, new_hash in hash_updates:
                watch = session.get(FeatureWatchModel, watch_id)
                if watch:
                    watch.last_notified_hash = new_hash


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def get_subscription_by_unsubscribe_token(engine, token: str) -> Optional[EmailSubscriptionModel]:
    """Look up a subscription by its unsubscribe_token."""
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        sub = session.scalar(
            select(EmailSubscriptionModel).where(EmailSubscriptionModel.unsubscribe_token == token)
        )
        if sub:
            session.expunge(sub)
        return sub


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def get_verified_subscription_by_email(engine, email: str) -> Optional[EmailSubscriptionModel]:
    """Look up a verified, active subscription by email address."""
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        sub = session.scalar(
            select(EmailSubscriptionModel)
            .where(EmailSubscriptionModel.email == email.strip().lower())
            .where(EmailSubscriptionModel.is_verified == True)
            .where(EmailSubscriptionModel.is_active == True)
        )
        if sub:
            session.expunge(sub)
        return sub


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def update_subscription_preferences(engine, token: str, preferences: Dict[str, Any]) -> bool:
    """Update subscription preferences identified by unsubscribe_token.

    Accepted keys in *preferences*: email_cadence, product_filter, release_type_filter, release_status_filter.
    Returns True if subscription found and updated.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)
    allowed_keys = {'email_cadence', 'product_filter', 'release_type_filter', 'release_status_filter'}
    with SessionLocal() as session:
        with session.begin():
            sub = session.scalar(
                select(EmailSubscriptionModel).where(EmailSubscriptionModel.unsubscribe_token == token)
            )
            if not sub:
                return False
            for key, value in preferences.items():
                if key in allowed_keys:
                    setattr(sub, key, value)
    return True


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def get_feature_watches_for_subscription(engine, subscription_id: str) -> List[Dict[str, Any]]:
    """Return all feature watches for a subscription with release details."""
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        rows = session.execute(
            select(FeatureWatchModel, ReleaseItemModel.feature_name, ReleaseItemModel.product_name)
            .join(ReleaseItemModel, FeatureWatchModel.release_item_id == ReleaseItemModel.release_item_id)
            .where(FeatureWatchModel.subscription_id == subscription_id)
            .order_by(FeatureWatchModel.created_at.desc())
        ).all()
        return [
            {
                'watch_id': w.id,
                'release_item_id': w.release_item_id,
                'feature_name': name,
                'product_name': product,
                'created_at': w.created_at.isoformat() if w.created_at else None,
            }
            for w, name, product in rows
        ]


@retry_on_transient_errors(max_attempts=3, initial_delay=0.5, backoff=2.0, max_delay=10.0)
def get_subscriptions_with_changed_watches(engine) -> List[Tuple[EmailSubscriptionModel, List[Dict[str, Any]]]]:
    """Return all active, verified subscribers that have watched features with hash mismatches.

    Used by the hourly email job to send watch alert emails.
    Returns list of (subscription, [changed_watch_dicts]).

    Uses a single joined query to avoid N+1 DB round-trips.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)
    with SessionLocal() as session:
        rows = session.execute(
            select(EmailSubscriptionModel, FeatureWatchModel, ReleaseItemModel)
            .join(FeatureWatchModel, EmailSubscriptionModel.id == FeatureWatchModel.subscription_id)
            .join(ReleaseItemModel, FeatureWatchModel.release_item_id == ReleaseItemModel.release_item_id)
            .where(EmailSubscriptionModel.is_active == True)
            .where(EmailSubscriptionModel.is_verified == True)
            .where(
                or_(
                    FeatureWatchModel.last_notified_hash == None,
                    FeatureWatchModel.last_notified_hash != ReleaseItemModel.row_hash,
                )
            )
        ).all()

        subs_by_id: Dict[str, EmailSubscriptionModel] = {}
        changes_by_sub_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        for sub, watch, release in rows:
            if sub.id not in subs_by_id:
                subs_by_id[sub.id] = sub
            changes_by_sub_id[sub.id].append({
                'watch_id': watch.id,
                'release_item_id': release.release_item_id,
                'feature_name': release.feature_name,
                'product_name': release.product_name,
                'release_type': release.release_type,
                'release_status': release.release_status,
                'feature_description': release.feature_description,
                'last_modified': release.last_modified.isoformat() if release.last_modified else None,
                'active': release.active,
                'current_hash': release.row_hash,
            })

        session.expunge_all()

    return [
        (subs_by_id[sub_id], changes_by_sub_id[sub_id])
        for sub_id in subs_by_id
    ]

@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def fetch_history_rows(engine, release_item_id: str):
    """Call stored procedure to retrieve history rows and shape output.
    Exposed fields: changed_columns (array) and last_modified for each version.
    We attempt to split ChangedColumns on comma, trimming whitespace.
    """

    conn = engine.raw_connection()
    try:
        # Assuming SQL Server; call the stored proc with one parameter
        cursor = conn.cursor()
        # Execute stored procedure. Adjust call pattern if needed.
        cursor.execute("EXEC [dbo].[GetReleaseItemHistoryById] @ReleaseItemId = ?", (release_item_id,))
        columns = [col[0] for col in cursor.description]
        # Expect: VersionNum, release_item_id, ChangedColumns, last_modified
        out = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            changed_raw = row_dict.get("ChangedColumns") or ""
            # Convert comma separated to list (ignore empty) preserving order
            changed_list = [c.strip() for c in changed_raw.split(',') if c and c.strip()]
            out.append({
                "changed_columns": changed_list,
                "last_modified": row_dict.get("last_modified").isoformat() if hasattr(row_dict.get("last_modified"), 'isoformat') and row_dict.get("last_modified") else None
            })
        # Sort descending by last_modified (fallback to insertion order if None)
        out.sort(key=lambda r: r.get("last_modified") or "", reverse=True)
        return out
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()

@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def vector_search_releases(
    engine,
    query_vector: List[float],
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    product_name: Optional[str] = None,
    release_type: Optional[str] = None,
    release_status: Optional[str] = None,
    modified_within_days: Optional[int] = None,
    include_inactive: bool = False,
) -> List[Dict[str, Any]]:
    """Search releases ordered by cosine similarity to query_vector."""
    vector_json = json.dumps(query_vector)

    conditions = ["release_vector IS NOT NULL"]
    filter_params: List[Any] = []

    if not include_inactive:
        conditions.append("active = 1")
    if product_name is not None:
        conditions.append("product_name = ?")
        filter_params.append(product_name)
    if release_type is not None:
        conditions.append("release_type = ?")
        filter_params.append(release_type)
    if release_status is not None:
        conditions.append("release_status = ?")
        filter_params.append(release_status)
    if modified_within_days is not None:
        cutoff = (datetime.utcnow() - timedelta(days=modified_within_days)).date()
        conditions.append("last_modified >= ?")
        filter_params.append(cutoff)

    where_sql = " AND ".join(conditions)

    sql = f"""
    SELECT release_item_id, feature_name, release_date, release_type,
           release_status, product_id, product_name, feature_description,
           blog_title, blog_url, last_modified, active,
           VECTOR_DISTANCE('cosine', release_vector, CAST(CAST(? AS NVARCHAR(MAX)) AS VECTOR(1536))) AS distance
    FROM release_items
    WHERE {where_sql}
    ORDER BY distance ASC
    OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """

    # Param order: vector (SELECT), filters (WHERE), offset & limit (pagination)
    all_params = [vector_json] + filter_params + [offset or 0, limit or 50]

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, all_params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()


@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def count_vector_search_releases(
    engine,
    product_name: Optional[str] = None,
    release_type: Optional[str] = None,
    release_status: Optional[str] = None,
    modified_within_days: Optional[int] = None,
    include_inactive: bool = False,
) -> int:
    """Count vectorized releases matching the given filters."""
    conditions = ["release_vector IS NOT NULL"]
    params: List[Any] = []

    if not include_inactive:
        conditions.append("active = 1")
    if product_name is not None:
        conditions.append("product_name = ?")
        params.append(product_name)
    if release_type is not None:
        conditions.append("release_type = ?")
        params.append(release_type)
    if release_status is not None:
        conditions.append("release_status = ?")
        params.append(release_status)
    if modified_within_days is not None:
        cutoff = (datetime.utcnow() - timedelta(days=modified_within_days)).date()
        conditions.append("last_modified >= ?")
        params.append(cutoff)

    where_sql = " AND ".join(conditions)
    sql = f"SELECT COUNT(*) FROM release_items WHERE {where_sql}"

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return cursor.fetchone()[0]
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()


@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def healthcheck(engine):
    conn = engine.raw_connection()
    try:
        # Assuming SQL Server; call the stored proc with one parameter
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        return True
    except:
        return False
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()
    
    