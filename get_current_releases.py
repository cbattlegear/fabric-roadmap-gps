import requests
import re
import unicodedata
import json
import logging
import os

from unidecode import unidecode

from lib.release_item import ReleaseItem
from db.db_sqlserver import make_engine, init_db, save_releases
from db.redis_cache import RedisCache

# Import the `configure_azure_monitor()` function from the
# `azure.monitor.opentelemetry` package.
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor


logger_name = 'fabric-gps-refresh'

opentelemetery_logger_name = f'{logger_name}.opentelemetry'

if os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING") and os.getenv("CURRENT_ENVIRONMENT") != "development":
    configure_azure_monitor(
        logger_name=opentelemetery_logger_name,
        enable_live_metrics=True 
    )

otelLogger= logging.getLogger(opentelemetery_logger_name)
stream = logging.StreamHandler()
otelLogger.addHandler(stream)
otelLogger.setLevel(logging.INFO)
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

        stats = save_releases(engine, items)
        print("DB save stats:", stats)
        change_count += stats['updated'] + stats['inserted']

    if change_count > 0:
        # Cache settings: 24h fresh + 24h stale-while-revalidate window
        _TTL_SECONDS = 24 * 60 * 60  # 24 hours fresh
        _STALE_TTL_SECONDS = _TTL_SECONDS  # additional stale window
        _LOCK_TTL_SECONDS = 60  # lock TTL to avoid stampede during refresh

        # Redis cache instance (fresh = 24h, stale = 24h)
        CACHE = RedisCache(ttl_seconds=_TTL_SECONDS, lock_ttl_seconds=_LOCK_TTL_SECONDS, stale_ttl_seconds=_STALE_TTL_SECONDS)
        CACHE.flush()

    otelLogger.info(f'Fabric-GPS Database Refresh completed with {change_count} changes')