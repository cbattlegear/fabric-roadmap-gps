#!/usr/bin/env python3
"""Generate a Reddit-formatted markdown report of Microsoft Fabric roadmap
changes from the past week, sourced from the Fabric GPS API.

Usage:
    python generate_reddit_report.py
    python generate_reddit_report.py --base-url http://localhost:8000
    python generate_reddit_report.py --days 14 -o report.md
"""

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime


def _fetch_json(url: str):
    """Fetch JSON from a URL using only stdlib."""
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _fetch_all_modified_releases(base_url: str, days: int = 7):
    """Paginate through all releases modified within the given window."""
    items = []
    page = 1
    while True:
        params = urllib.parse.urlencode({
            "modified_within_days": days,
            "include_inactive": "true",
            "page": page,
            "page_size": 200,
            "sort": "product_name",
        })
        data = _fetch_json(f"{base_url}/api/releases?{params}")
        items.extend(data["data"])
        if not data["pagination"]["has_next"]:
            break
        page += 1
        if page > 50:
            break
    return items


def _fetch_history(base_url: str, release_item_id: str):
    """Fetch change history for a single release item."""
    url = f"{base_url}/api/releases/history/{urllib.parse.quote(release_item_id)}"
    try:
        return _fetch_json(url)
    except (urllib.error.HTTPError, urllib.error.URLError):
        return None


def _categorize_items(items, base_url: str):
    """Sort every modified item into exactly one bucket.

    Priority order:
      1. Removed   – active is False
      2. New       – only one history version (just added to roadmap)
      3. Delayed   – release_date listed in changed_columns of latest history
      4. GA        – release_type == "General availability"
      5. Preview   – everything else (including unknown types)
    """
    new, preview, ga, delayed, removed = [], [], [], [], []
    total = len(items)

    for i, item in enumerate(items, 1):
        print(f"\r  Categorizing {i}/{total}…", end="", file=sys.stderr)

        if not item.get("active", True):
            removed.append(item)
            continue

        history = _fetch_history(base_url, item["release_item_id"])

        # History fetch failed — fall back to release_type categorization
        if history is None:
            _bucket_by_type(item, preview, ga)
            continue

        if len(history) <= 1:
            new.append(item)
            continue

        latest_changed = history[0].get("changed_columns", []) if history else []
        if "release_date" in latest_changed:
            delayed.append(item)
        else:
            _bucket_by_type(item, preview, ga)

    print(file=sys.stderr)
    return new, preview, ga, delayed, removed


def _bucket_by_type(item, preview_list, ga_list):
    """Append item to the GA or Preview bucket based on release_type."""
    if (item.get("release_type") or "").lower() == "general availability":
        ga_list.append(item)
    else:
        preview_list.append(item)


def _group_by_workload(items):
    """Group items by product_name, sorted alphabetically."""
    groups = defaultdict(list)
    for item in items:
        groups[item.get("product_name") or "Unknown"].append(item)
    return dict(sorted(groups.items()))


def _format_item(item, base_url: str) -> str:
    """Format a single item as a Reddit markdown bullet."""
    rid = item["release_item_id"]
    name = item.get("feature_name") or "Unnamed Feature"
    link = f"{base_url}/release/{rid}"

    parts = []
    if item.get("release_status"):
        parts.append(item["release_status"])
    if item.get("release_date"):
        try:
            d = datetime.strptime(item["release_date"], "%Y-%m-%d")
            parts.append(d.strftime("%b %Y"))
        except ValueError:
            parts.append(item["release_date"])

    suffix = f" ({', '.join(parts)})" if parts else ""
    return f"- [{name}]({link}){suffix}"


def _render_section(title: str, items, base_url: str) -> str:
    """Render a full section with workload sub-groups."""
    if not items:
        return ""

    lines = [f"## {title}", ""]
    for workload, group in _group_by_workload(items).items():
        lines.append(f"**{workload}**")
        lines.append("")
        for item in sorted(group, key=lambda x: x.get("feature_name") or ""):
            lines.append(_format_item(item, base_url))
        lines.append("")

    return "\n".join(lines)


def generate_report(base_url: str, days: int = 7) -> str:
    """Fetch data from the Fabric GPS API and build the markdown report."""
    print(f"Fetching releases modified in the last {days} days…", file=sys.stderr)
    items = _fetch_all_modified_releases(base_url, days)
    print(f"  Found {len(items)} modified items.", file=sys.stderr)

    if not items:
        return "# Microsoft Fabric Roadmap Weekly Update\n\nNo changes detected this week."

    new, preview, ga, delayed, removed = _categorize_items(items, base_url)

    today = date.today().strftime("%B %d, %Y")
    lines = [
        f"# Microsoft Fabric Roadmap Weekly Update — {today}",
        "",
        f"*Changes from the last {days} days, sourced from [Fabric GPS]({base_url}).*",
        "",
    ]

    sections = [
        ("New Additions", new),
        ("Preview Updates", preview),
        ("General Availability Updates", ga),
        ("Delayed Items", delayed),
        ("Removed Items", removed),
    ]

    for title, bucket in sections:
        section = _render_section(title, bucket, base_url)
        if section:
            lines.append(section)

    lines.append("---")
    lines.append("")
    lines.append(
        f"**Summary:** {len(new)} new · {len(preview)} preview updates · "
        f"{len(ga)} GA updates · {len(delayed)} delayed · {len(removed)} removed · "
        f"{len(items)} total changes"
    )
    lines.append("")
    lines.append(f"*Generated from [Fabric GPS]({base_url})*")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate a Reddit markdown report of Fabric roadmap changes."
    )
    parser.add_argument(
        "--base-url",
        default="https://www.fabric-gps.com",
        help="Fabric GPS base URL (default: https://www.fabric-gps.com)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Look-back window in days (default: 7, max: 30)",
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file path (default: stdout)",
    )
    args = parser.parse_args()

    report = generate_report(args.base_url.rstrip("/"), args.days)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
