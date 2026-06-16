"""
Hero Product Inventory Check
=============================
Verifies Shopify availability for 5 hero products ahead of ad launch.

Usage:
    python check_hero_inventory.py

Writes full detail to hero_inventory_check.json and prints a report.
"""

import json
import os
import re
import sys
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path

# ---- Load .env (no python-dotenv dependency) -------------------------------
ENV_PATH = Path(__file__).parent / '.env'
if ENV_PATH.exists():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

SHOP = os.getenv('SHOPIFY_STORE', 'drruthroberts-com.myshopify.com')
if not SHOP.endswith('.myshopify.com'):
    SHOP = f"{SHOP}.myshopify.com"
TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN') or os.getenv('SHOPIFY_API_KEY')
API_VERSION = os.getenv('SHOPIFY_API_VERSION', '2024-01')

if not TOKEN:
    print("ERROR: No SHOPIFY_ACCESS_TOKEN found in .env")
    sys.exit(1)

GQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"
REST_BASE = f"https://{SHOP}/admin/api/{API_VERSION}"
HEADERS = {
    'X-Shopify-Access-Token': TOKEN,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}

# ---- Target hero products --------------------------------------------------
# Each target has a canonical name and a list of fuzzy-match tokens.
TARGETS = [
    {
        'key': 'HTBS',
        'canonical': 'HTBS (Holistic Total Body Support)',
        'tokens': ['htbs', 'holistic total body', 'total body support'],
    },
    {
        'key': 'DAILY_WELLNESS',
        'canonical': "Pet's Daily Wellness Formula",
        'tokens': ['daily wellness', "pet's daily", 'pets daily', 'wellness formula'],
    },
    {
        'key': 'ARTHRIPAWZ',
        'canonical': 'ArthriPawz',
        'tokens': ['arthripawz', 'arthripaws', 'arthri-pawz', 'arthri-paws', 'arthri pawz', 'arthri paws'],
    },
    {
        'key': 'OMEGAPAWZ',
        'canonical': 'OmegaPawz',
        'tokens': ['omegapawz', 'omegapaws', 'omega-pawz', 'omega-paws', 'omega pawz', 'omega paws'],
    },
    {
        'key': 'HISTAPAWZ',
        'canonical': 'HistaPawz',
        'tokens': ['histapawz', 'histapaws', 'hista-pawz', 'hista-paws', 'hista pawz', 'hista paws'],
    },
]


def _http_json(url, method='GET', payload=None):
    data = None
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=HEADERS, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode('utf-8')), None
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}: {e.read().decode('utf-8')[:300]}"
    except urllib.error.URLError as e:
        return None, f"URL error: {e}"


# ---- GraphQL fetch ---------------------------------------------------------
GQL_QUERY = """
{
  products(first: 50, query: "title:*Pawz* OR title:*Paws* OR title:*HTBS* OR title:*Total Body* OR title:*Daily Wellness* OR title:*Holistic*") {
    edges { node {
      id
      title
      handle
      status
      featuredImage { url altText }
      images(first: 5) { edges { node { url } } }
      totalInventory
      variants(first: 25) {
        edges { node {
          id
          title
          sku
          price
          inventoryQuantity
        }}
      }
    }}
  }
}
"""


def fetch_graphql():
    result, err = _http_json(GQL_URL, method='POST', payload={'query': GQL_QUERY})
    if err:
        print(f"[GraphQL] failed: {err}")
        return None
    if 'errors' in result:
        print(f"[GraphQL] errors: {result['errors']}")
        return None
    edges = result.get('data', {}).get('products', {}).get('edges', [])
    products = []
    for edge in edges:
        n = edge['node']
        variants = [v['node'] for v in n.get('variants', {}).get('edges', [])]
        images = [i['node']['url'] for i in n.get('images', {}).get('edges', [])]
        total_inv = n.get('totalInventory')
        if total_inv is None:
            total_inv = sum((v.get('inventoryQuantity') or 0) for v in variants)
        products.append({
            'id': n['id'],
            'title': n['title'],
            'handle': n['handle'],
            'status': (n.get('status') or '').lower(),
            'featured_image': (n.get('featuredImage') or {}).get('url'),
            'images': images,
            'total_inventory': total_inv,
            'variants': [
                {
                    'id': v['id'],
                    'title': v.get('title'),
                    'sku': v.get('sku'),
                    'price': v.get('price'),
                    'inventory_quantity': v.get('inventoryQuantity'),
                }
                for v in variants
            ],
        })
    return products


# ---- REST fallback ---------------------------------------------------------
def fetch_rest():
    """Pull all products via REST and filter client-side."""
    all_products = []
    url = f"{REST_BASE}/products.json?limit=250&fields=id,title,handle,status,image,images,variants"
    while url:
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode('utf-8'))
                all_products.extend(body.get('products', []))
                link = resp.headers.get('Link', '')
                url = None
                if 'rel="next"' in link:
                    for part in link.split(','):
                        if 'rel="next"' in part:
                            url = part.split('<')[1].split('>')[0]
                            break
        except urllib.error.HTTPError as e:
            print(f"[REST] failed: {e.code} {e.read().decode()[:200]}")
            break

    # Collect inventory item IDs and get levels
    inv_item_ids = []
    for p in all_products:
        for v in p.get('variants', []):
            iid = v.get('inventory_item_id')
            if iid:
                inv_item_ids.append(iid)

    inventory_map = {}
    for i in range(0, len(inv_item_ids), 50):
        batch = inv_item_ids[i:i + 50]
        q = urllib.parse.urlencode({'inventory_item_ids': ','.join(str(x) for x in batch), 'limit': 250})
        inv_url = f"{REST_BASE}/inventory_levels.json?{q}"
        data, err = _http_json(inv_url)
        if err:
            print(f"[REST inventory] {err}")
            continue
        for lvl in data.get('inventory_levels', []):
            iid = lvl['inventory_item_id']
            inventory_map.setdefault(iid, 0)
            inventory_map[iid] += (lvl.get('available') or 0)

    products = []
    for p in all_products:
        variants = []
        total = 0
        for v in p.get('variants', []):
            iid = v.get('inventory_item_id')
            qty = inventory_map.get(iid, v.get('inventory_quantity', 0) or 0)
            total += qty
            variants.append({
                'id': v.get('id'),
                'title': v.get('title'),
                'sku': v.get('sku'),
                'price': v.get('price'),
                'inventory_quantity': qty,
            })
        images = [img.get('src') for img in p.get('images', [])]
        featured = (p.get('image') or {}).get('src')
        products.append({
            'id': p['id'],
            'title': p['title'],
            'handle': p['handle'],
            'status': (p.get('status') or '').lower(),
            'featured_image': featured,
            'images': images,
            'total_inventory': total,
            'variants': variants,
        })
    return products


# ---- Matching --------------------------------------------------------------
def _normalize(s):
    return re.sub(r'[^a-z0-9 ]+', ' ', (s or '').lower())


def match_targets(products):
    matches = {t['key']: [] for t in TARGETS}
    for p in products:
        norm = _normalize(p['title']) + ' ' + _normalize(p['handle'])
        for t in TARGETS:
            for tok in t['tokens']:
                if _normalize(tok) in norm:
                    matches[t['key']].append(p)
                    break
    # Dedupe by product id within each bucket
    for k, ps in matches.items():
        seen = set()
        out = []
        for p in ps:
            if p['id'] not in seen:
                seen.add(p['id'])
                out.append(p)
        matches[k] = out
    return matches


# ---- Stock status ----------------------------------------------------------
def stock_status(total):
    if total is None:
        return 'UNKNOWN'
    if total <= 0:
        return 'OUT_OF_STOCK'
    if total < 20:
        return 'LOW_STOCK'
    return 'IN_STOCK'


# ---- Main ------------------------------------------------------------------
def main():
    print(f"Shop: {SHOP}  API: {API_VERSION}")
    # Always use REST to pull the full catalog — GraphQL title:* search is
    # too narrow (misses products whose hero name lives in tags/vendor only).
    print("Pulling full product catalog via REST...")
    products = fetch_rest()
    source = 'rest'
    print(f"Retrieved {len(products)} candidate products via {source}.")

    matches = match_targets(products)

    report = {
        'shop': SHOP,
        'source': source,
        'products_scanned': len(products),
        'targets': [],
    }

    print("\n" + "=" * 70)
    print("HERO PRODUCT INVENTORY REPORT")
    print("=" * 70)

    blockers = []
    for t in TARGETS:
        found = matches[t['key']]
        if not found:
            report['targets'].append({
                'key': t['key'],
                'canonical': t['canonical'],
                'found': False,
                'searched_tokens': t['tokens'],
            })
            print(f"\n[NOT FOUND] {t['canonical']}")
            print(f"  Searched: {', '.join(t['tokens'])}")
            blockers.append(t['canonical'])
            continue

        for p in found:
            status = stock_status(p.get('total_inventory'))
            entry = {
                'key': t['key'],
                'canonical': t['canonical'],
                'found': True,
                'shopify_title': p['title'],
                'handle': p['handle'],
                'product_id': p['id'],
                'product_status': p['status'],
                'total_inventory': p['total_inventory'],
                'stock_status': status,
                'featured_image': p['featured_image'],
                'images': p['images'],
                'variants': p['variants'],
            }
            report['targets'].append(entry)
            print(f"\n[FOUND] {t['canonical']}")
            print(f"  Shopify title : {p['title']}")
            print(f"  Handle        : {p['handle']}")
            print(f"  Status        : {p['status']}  |  Stock: {status} ({p['total_inventory']} units)")
            print(f"  Image         : {p['featured_image']}")
            print(f"  Variants      : {len(p['variants'])}")
            for v in p['variants']:
                print(f"    - {v['title']}  SKU={v['sku']}  ${v['price']}  qty={v['inventory_quantity']}")
            if status in ('OUT_OF_STOCK',) or p['status'] != 'active':
                blockers.append(f"{t['canonical']} ({status}, product_status={p['status']})")

    # Verdict
    report['blockers'] = blockers
    print("\n" + "=" * 70)
    if blockers:
        print("VERDICT: NOT SAFE TO LAUNCH — blockers:")
        for b in blockers:
            print(f"  - {b}")
        report['verdict'] = 'NOT_SAFE'
    else:
        print("VERDICT: SAFE TO LAUNCH")
        report['verdict'] = 'SAFE'
    print("=" * 70)

    out_path = Path(__file__).parent / 'hero_inventory_check.json'
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nFull detail saved to: {out_path}")


if __name__ == '__main__':
    main()
