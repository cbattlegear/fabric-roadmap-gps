import os
import json
import uuid
import hashlib
from datetime import datetime, date, timedelta
from typing import Iterable, Any, Tuple, Dict
import urllib.parse
import time
import random
import logging

# ...existing code...
from sqlalchemy import (
    create_engine, Column, String, Integer, Date, Boolean, Text, MetaData, func, select, or_
)
from sqlalchemy.dialects.mssql import DATETIME2
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
    Returns counts: {'inserted': n, 'updated': m, 'unchanged': k}
    This function is retried on transient SQL/Azure connection errors.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)
    inserted = updated = unchanged = 0

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
                    )
                    session.add(model)
                    inserted += 1
                else:
                    if (existing.row_hash or "") == new_hash:
                        unchanged += 1
                    else:
                        # update only when changed
                        for k, v in row_values.items():
                            setattr(existing, k, v)
                        existing.row_hash = new_hash
                        existing.last_modified = now
                        updated += 1

    return {"inserted": inserted, "updated": updated, "unchanged": unchanged}

@retry_on_transient_errors(max_attempts=5, initial_delay=1.0, backoff=2.0, max_delay=60.0)
def get_recently_modified_releases(
    engine,
    limit: Optional[int] = None,
    product_name: Optional[str] = None,
    release_type: Optional[str] = None,
    release_status: Optional[str] = None,
    modified_within_days: Optional[int] = None,
    q: Optional[str] = None,
) -> List[ReleaseItemModel]:
    """
    Return up to `limit` releases most recently modified (by last_modified desc).
    Optionally filter by exact `product_name`, `release_type`, or `release_status`
    (direct equality). Results are ordered by last_modified desc, then release_date desc.

    Supports optional text search via `q` which performs a case-insensitive partial match
    against feature_name, feature_description, and product_name.
    """
    SessionLocal = sessionmaker(bind=engine, future=True)

    stmt = select(ReleaseItemModel)
    filters = []
    # use direct equality instead of ilike/partial match for explicit filters
    if product_name is not None:
        filters.append(ReleaseItemModel.product_name == product_name)
    if release_type is not None:
        filters.append(ReleaseItemModel.release_type == release_type)
    if release_status is not None:
        filters.append(ReleaseItemModel.release_status == release_status)
    if modified_within_days is not None:
        modified_since = datetime.now() - timedelta(days=modified_within_days)
        filters.append(ReleaseItemModel.last_modified >= modified_since)

    # Text search (case-insensitive partial match) across useful text columns
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

    if limit is not None:
        stmt = stmt.order_by(ReleaseItemModel.last_modified.desc(), ReleaseItemModel.release_date.desc()).limit(limit)
    else:
        stmt = stmt.order_by(ReleaseItemModel.last_modified.desc(), ReleaseItemModel.release_date.desc())

    with SessionLocal() as session:
        rows = session.scalars(stmt).all()

    return rows

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