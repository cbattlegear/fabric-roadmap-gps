import requests
import re
import unicodedata
import json
import os

from unidecode import unidecode

from lib.release_item import ReleaseItem
from db.db_sqlserver import make_engine, init_db, save_releases, deactivate_missing_releases
from lib.indexnow import submit_urls
from lib.telemetry import init_telemetry

otelLogger = init_telemetry("fabric-gps-refresh")
otelLogger.info('Fabric-GPS Database Refresh started')

def extract_product_families(js_text):
    # match blocks like:
    # ProductFamily = new Object();
    # ProductFamily.id = "GUID";
    # ProductFamily.name = "Name";
    pattern = re.compile(
        r'ProductFamily\s*=\s*new\s+Object\(\)\s*;\s*'
        r'ProductFamily\.id\s*=\s*["\']([^"\']+)["\']\s*;\s*'
        r'ProductFamily\.name\s*=\s*["\']([^"\']+)["\']',
        re.DOTALL
    )
    return [{'id': mid.strip(), 'name': mname.strip()} for mid, mname in pattern.findall(js_text)]

if __name__ == '__main__':
    # build engine (uses SQLSERVER_CONN env var by default)
    engine = make_engine()
    # create table if missing
    init_db(engine)

    r = requests.get('https://roadmap.fabric.microsoft.com')
    r.raise_for_status()
    families = extract_product_families(r.text)
    product_releases = {}
    all_fetched_ids: set = set()
    all_changed_ids: list = []
    change_count = 0
    for product in families:
        print(f"Product Family: {product['name']} (ID: {product['id']})")
        
        raw_json = requests.get(f'https://roadmap.fabric.microsoft.com/fabric-json/?productId={product["id"]}')
        raw_json.raise_for_status()

        raw_json = unidecode(raw_json.content.decode('utf-8')
                             .replace(' ""', " '")
                             .replace('"" ', "' ")
                             .replace('“', "'")
                             .replace('”', "'")
                             .replace("\t", " ")
                             .replace("\_", "_")
                            )
        release_items = []
        try:
            release_items = json.loads(raw_json)['results']
        except json.JSONDecodeError as e:
            pos = e.pos               # 0-based char index
            lineno = e.lineno
            colno = e.colno
            print(f"JSONDecodeError: {e.msg} at line {lineno} column {colno} (char {pos})")

            # show the full line and point at the column (if available)
            lines = raw_json.splitlines()
            if 1 <= lineno <= len(lines):
                line_text = lines[lineno - 1]
                print(f"Line {lineno}:")
                print(line_text.rstrip('\n'))
                print(' ' * (colno - 1) + '^')
            else:
                start = max(0, pos - 80)
                end = min(len(raw_json), pos + 80)
                snippet = raw_json[start:end]
                print("Snippet:")
                print(snippet)
                print(' ' * (pos - start) + '^')

            # show the offending character in several forms
            if 0 <= pos < len(raw_json):
                ch = raw_json[pos]
                print("Offending char (repr):", repr(ch))
                print("ord:", ord(ch), "hex:", hex(ord(ch)))
                print("unicode name:", unicodedata.name(ch, "UNKNOWN"))
                print("unicode category:", unicodedata.category(ch))
            else:
                print("Position out of range:", pos)

            raise  # re-raise after diagnostics (or use sys.exit/continue as needed)

        items = [ReleaseItem.from_dict(r) for r in release_items]
        product_releases[product['id']] = items

        # Track all IDs seen in this fetch for removal detection
        for item in items:
            rid = str(item.release_item_id) if item.release_item_id else None
            if rid:
                all_fetched_ids.add(rid)

        stats = save_releases(engine, items)
        print("DB save stats:", stats)
        change_count += stats['updated'] + stats['inserted']
        all_changed_ids.extend(stats.get('changed_ids', []))

    # Mark releases no longer on the roadmap as inactive
    deactivation_stats = deactivate_missing_releases(engine, all_fetched_ids)
    print("Deactivation stats:", deactivation_stats)
    change_count += deactivation_stats['deactivated']
    all_changed_ids.extend(deactivation_stats.get('deactivated_ids', []))

    # Notify search engines via IndexNow
    if all_changed_ids:
        unique_ids = list(dict.fromkeys(all_changed_ids))  # dedupe, preserve order
        otelLogger.info(f'Submitting {len(unique_ids)} changed URLs to IndexNow')
        submit_urls(unique_ids)

    otelLogger.info(f'Fabric-GPS Database Refresh completed with {change_count} changes')