import requests
import re
import unicodedata
import json

from unidecode import unidecode

from release_item import ReleaseItem
from db_sqlserver import get_recently_modified_releases, make_engine, init_db, save_releases

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

    rows = get_recently_modified_releases(engine)
    for r in rows:
        print(r.release_item_id, r.feature_name, r.release_date, r.last_modified)