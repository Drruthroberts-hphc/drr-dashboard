#!/usr/bin/env python3
"""Search all Shopify theme files for 'hyros' (case-insensitive)."""
import os
import re
import requests
from dotenv import load_dotenv
load_dotenv()

shop = os.getenv('SHOPIFY_STORE', 'drruthroberts-com.myshopify.com')
token = os.getenv('SHOPIFY_ACCESS_TOKEN')
api = f'https://{shop}/admin/api/2024-01'
headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}

# Use theme ID from URL
theme_id = 137537290305
print(f'Using theme ID: {theme_id}')

# Get all assets
print('\n=== Listing theme assets ===')
r = requests.get(f'{api}/themes/{theme_id}/assets.json', headers=headers)
assets = r.json().get('assets', [])
liquid_assets = [a for a in assets if a['key'].endswith('.liquid') or a['key'].endswith('.json') or a['key'].endswith('.js')]
print(f'Total assets: {len(assets)} | Liquid/JSON/JS files: {len(liquid_assets)}')

# Search each
print('\n=== Searching for "hyros" in each file ===\n')
PATTERNS = re.compile(r'hyros', re.IGNORECASE)
found_count = 0
for a in liquid_assets:
    # Fetch file content
    r = requests.get(f'{api}/themes/{theme_id}/assets.json', headers=headers,
                     params={'asset[key]': a['key']})
    if r.status_code != 200:
        continue
    asset_data = r.json().get('asset', {})
    content = asset_data.get('value', '') or asset_data.get('attachment', '')
    if not content:
        continue
    # Search
    matches = []
    for i, line in enumerate(content.split('\n'), 1):
        if PATTERNS.search(line):
            matches.append((i, line.strip()[:200]))
    if matches:
        found_count += 1
        print(f'📄 {a["key"]}')
        for line_num, line in matches:
            print(f'    Line {line_num}: {line}')
        print()

if found_count == 0:
    print('✅ No "hyros" found in any theme file.')
else:
    print(f'\nFound in {found_count} file(s).')

# Also check Online Store > Preferences > Additional Scripts
print('\n=== Shop-level additional scripts (Preferences) ===')
r = requests.get(f'{api}/shop.json', headers=headers)
shop_data = r.json().get('shop', {})
google_analytics = shop_data.get('google_analytics', '') or ''
if 'hyros' in google_analytics.lower():
    print(f'⚠️ Hyros found in Google Analytics field:\n{google_analytics[:500]}')
else:
    print('No Hyros in google_analytics field.')

# Check checkout-related scripts via script tags (alternative location)
print('\n=== Script tags (apps inject these) ===')
r = requests.get(f'{api}/script_tags.json', headers=headers)
script_tags = r.json().get('script_tags', []) if r.status_code == 200 else []
hyros_tags = [s for s in script_tags if 'hyros' in s.get('src', '').lower()]
if hyros_tags:
    print(f'⚠️ Found {len(hyros_tags)} Hyros script tag(s):')
    for s in hyros_tags:
        print(f'  ID: {s["id"]} | src: {s["src"]} | display_scope: {s.get("display_scope")}')
else:
    print(f'No Hyros script tags found. Total script tags installed: {len(script_tags)}')
    if script_tags:
        print('Other script tags installed:')
        for s in script_tags[:10]:
            print(f'  - {s.get("src", "")[:100]}')
