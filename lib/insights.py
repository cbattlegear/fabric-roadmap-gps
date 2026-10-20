"""Bounded public query contract for observed roadmap insights."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlencode


METRICS = ("all", "addition", "edit", "removal", "restoration", "status", "target", "upcoming")
PAGE_SIZE = 50


@dataclass(frozen=True)
class InsightsQuery:
    days: int
    start: date
    end: date
    product_name: str | None = None
    metric: str = "all"
    bucket: str | None = None
    page: int = 1

    def url(self, **changes):
        params = {
            "days": self.days, "end": self.end.isoformat(),
            "product_name": self.product_name, "metric": self.metric,
            "bucket": self.bucket, "page": self.page,
        }
        params.update(changes)
        return "/insights?" + urlencode({k: v for k, v in params.items() if v is not None})


def parse_insights_query(args, today=None):
    today = today or datetime.now(timezone.utc).date()
    try:
        days = int(args.get("days", "30"))
        page = int(args.get("page", "1"))
    except (ValueError, TypeError):
        raise ValueError("days and page must be integers") from None
    if days not in (30, 90) or not 1 <= page <= 200:
        raise ValueError("days must be 30 or 90; page must be between 1 and 200")
    end_raw = args.get("end", (today + timedelta(days=1)).isoformat())
    try:
        end = date.fromisoformat(end_raw)
    except (ValueError, TypeError):
        raise ValueError("end must be YYYY-MM-DD (exclusive, UTC)") from None
    if end.isoformat() != end_raw or not today - timedelta(days=366) <= end <= today + timedelta(days=1):
        raise ValueError("end must be canonical YYYY-MM-DD within the past year or tomorrow")
    product = args.get("product_name") or None
    if product and len(product) > 200:
        raise ValueError("product_name must not exceed 200 characters")
    metric = args.get("metric", "all")
    if metric not in METRICS:
        raise ValueError("Unknown insights metric")
    bucket = args.get("bucket") or None
    if bucket:
        try:
            parsed = date.fromisoformat(bucket)
        except ValueError:
            parsed = None
        is_day = parsed is not None and parsed.isoformat() == bucket
        is_quarter = len(bucket) == 7 and bucket[:4].isdigit() and bucket[4:6] == "-Q" and bucket[6] in "1234"
        if not ((metric == "status" and is_day and end - timedelta(days=days) <= parsed < end)
                or (metric == "upcoming" and (is_quarter or bucket == "unknown"))):
            raise ValueError("bucket must match a status day or upcoming quarter")
    return InsightsQuery(days, end - timedelta(days=days), end, product, metric, bucket, page)


def prepare_insights(data, query):
    """Attach exact filtered links without reconstructing event sets in the browser."""
    for row in data["activity"]:
        row["url"] = query.url(product_name=row["product_name"], metric=row["kind"], bucket=None, page=1)
    for row in data["transitions"]:
        row["url"] = query.url(metric="status", bucket=row["day"], page=1)
    for row in data["upcoming"]:
        row["url"] = query.url(metric="upcoming", bucket=row["bucket"], page=1)
    data["filters"] = {
        "days": query.days, "start": query.start.isoformat(), "end": query.end.isoformat(),
        "product_name": query.product_name, "metric": query.metric, "bucket": query.bucket,
        "page": query.page, "page_size": PAGE_SIZE,
    }
    data["coverage"]["complete"] = False
    data["coverage"]["note"] = (
        "Observed retained temporal history only; retention and initial import coverage are not verified. "
        "Additions are first retained observations, not confirmed launch dates. "
        "Baseline versions at the window boundary are not activity. Missing predecessors cannot establish transitions."
    )
    data["details"]["previous_url"] = query.url(page=query.page - 1) if query.page > 1 else None
    data["details"]["next_url"] = (
        query.url(page=query.page + 1)
        if query.page < 200 and query.page * PAGE_SIZE < data["details"]["total"] else None
    )
    data["details"]["capped"] = data["details"]["total"] > 200 * PAGE_SIZE
    products = {}
    kinds = ("addition", "edit", "removal", "restoration")
    for row in data["activity"]:
        product = products.setdefault(row["product_name"], {
            "name": row["product_name"], "events": 0, "segments": [],
            "url": query.url(product_name=row["product_name"], metric="all", bucket=None, page=1),
        })
        product["events"] += row["events"]
        product["segments"].append(row)
    product_rows = sorted(products.values(), key=lambda row: (-row["events"], row["name"]))
    max_product = max((row["events"] for row in product_rows), default=1)
    for product in product_rows:
        product["width"] = 100 * product["events"] / max_product
        product["segments"].sort(key=lambda row: kinds.index(row["kind"]))
        for row in product["segments"]:
            row["width"] = 100 * row["events"] / product["events"] if product["events"] else 0

    by_day = {row["day"]: row for row in data["transitions"]}
    daily = []
    max_status = max((row["events"] for row in data["transitions"]), default=1)
    for index in range(query.days):
        day = (query.start + timedelta(days=index)).isoformat()
        count = by_day.get(day, {}).get("events", 0)
        daily.append({
            "day": day, "events": count,
            "x": round(8 + index * 584 / max(1, query.days - 1), 2),
            "y": round(160 - 140 * count / max(1, max_status), 2),
            "url": query.url(metric="status", bucket=day, page=1),
        })
    total_events = sum(row["events"] for row in data["activity"])
    upcoming_max = max((row["features"] for row in data["upcoming"]), default=1)
    for row in data["upcoming"]:
        row["width"] = 100 * row["features"] / max(1, upcoming_max)
    data["visuals"] = {
        "products": product_rows, "events": total_events,
        "status_events": sum(row["events"] for row in data["transitions"]),
        "status_points": " ".join(f'{row["x"]},{row["y"]}' for row in daily),
        "status_days": [row for row in daily if row["events"]],
        "status_max": max_status,
        "status_end": (query.end - timedelta(days=1)).strftime("%b %d"),
        "target_percent": round(100 * data["target_changes"] / total_events, 1) if total_events else 0,
        "upcoming_features": sum(row["features"] for row in data["upcoming"]),
    }
    return data
