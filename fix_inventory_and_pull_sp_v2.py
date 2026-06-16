#!/usr/bin/env python3
"""1) Set Daily Wellness inventory to 999.
   2) Identify Standard Process products + remove from Google channel.
"""
import os
import requests
import json
from dotenv import load_dotenv
load_dotenv()
from google.ads.googleads.client import GoogleAdsClient

shop = os.getenv('SHOPIFY_STORE', 'drruthroberts-com.myshopify.com')
token = os.getenv('SHOPIFY_ACCESS_TOKEN')
api = f'https://{shop}/admin/api/2024-01'
gql_url = f'https://{shop}/admin/api/2024-01/graphql.json'
headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}

config = {
    'developer_token': os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN'),
    'client_id': os.getenv('GOOGLE_ADS_CLIENT_ID'),
    'client_secret': os.getenv('GOOGLE_ADS_CLIENT_SECRET'),
    'refresh_token': os.getenv('GOOGLE_ADS_REFRESH_TOKEN'),
    'login_customer_id': os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID', '').replace('-',''),
    'use_proto_plus': True,
}
client = GoogleAdsClient.load_from_dict(config)
customer_id = '6868436418'
ga_service = client.get_service('GoogleAdsService')

# ===== STEP D FIRST: Daily Wellness inventory =====
print('=' * 70)
print('STEP 1: Set Daily Wellness inventory to 999')
print('=' * 70)
r = requests.get(
    f'{api}/products.json',
    headers=headers,
    params={'handle': 'pets-daily-wellness-formula-for-pets-with-pork-beef-allergy'}
)
products = r.json().get('products', [])
if products:
    dw = products[0]
    print(f'Found: {dw["title"]} (ID {dw["id"]})')
    for variant in dw.get('variants', []):
        print(f'  Variant SKU: {variant.get("sku")} | Current qty: {variant.get("inventory_quantity")}')
        inv_item_id = variant.get('inventory_item_id')
        r2 = requests.get(
            f'{api}/inventory_levels.json',
            headers=headers,
            params={'inventory_item_ids': inv_item_id}
        )
        levels = r2.json().get('inventory_levels', [])
        for level in levels:
            location_id = level.get('location_id')
            r3 = requests.post(
                f'{api}/inventory_levels/set.json',
                headers=headers,
                json={
                    'location_id': location_id,
                    'inventory_item_id': inv_item_id,
                    'available': 999,
                }
            )
            if r3.status_code in (200, 201):
                print(f'    ✅ Set qty=999 at location {location_id}')
            else:
                print(f'    ❌ Location {location_id}: {r3.status_code} {r3.text[:300]}')
else:
    print('Could not find Daily Wellness product')

# ===== STEP 2: Find Google channel publication ID =====
print('\n' + '=' * 70)
print('STEP 2: Find Google sales channel publication ID')
print('=' * 70)
gql_query = """{ publications(first: 25) { edges { node { id name } } } }"""
r = requests.post(gql_url, headers=headers, json={'query': gql_query})
print(f'GraphQL status: {r.status_code}')
print(f'Response: {json.dumps(r.json(), indent=2)[:1500]}')

# Try REST endpoint for publications as fallback
google_pub_id = None
try:
    data = r.json().get('data') or {}
    pubs = data.get('publications', {}).get('edges', [])
    for edge in pubs:
        name = edge['node']['name']
        pid = edge['node']['id']
        print(f'  Publication: {name} -> {pid}')
        if 'google' in name.lower():
            google_pub_id = pid
except Exception as e:
    print(f'GraphQL parse error: {e}')

# Fallback to REST
if not google_pub_id:
    print('\nTrying REST endpoint /publications.json...')
    r_rest = requests.get(f'{api}/publications.json', headers=headers)
    print(f'REST status: {r_rest.status_code}')
    if r_rest.status_code == 200:
        for pub in r_rest.json().get('publications', []):
            name = pub.get('name', '')
            print(f'  Pub: {name} (ID {pub["id"]})')
            if 'google' in name.lower():
                google_pub_id = f'gid://shopify/Publication/{pub["id"]}'

print(f'\nGoogle channel ID: {google_pub_id}')

# ===== STEP 3: Identify ALL Standard Process products =====
print('\n' + '=' * 70)
print('STEP 3: Find ALL Standard Process products in catalog')
print('=' * 70)
# Use REST API to get products by vendor
all_sp = []
page_info = None
url = f'{api}/products.json?vendor=Standard%20Process%20Inc&limit=250&fields=id,title,vendor,handle'
r = requests.get(url, headers=headers)
products = r.json().get('products', [])
for p in products:
    all_sp.append(p)
    print(f'  [{p["id"]}] {p["title"][:60]} | Vendor: {p["vendor"]}')

print(f'\nTotal Standard Process products in store: {len(all_sp)}')

# ===== STEP 4: Identify SP products with current image issues =====
print('\n' + '=' * 70)
print('STEP 4: Cross-reference with Google Ads disapprovals')
print('=' * 70)
IMG_ISSUES = {'image_link_broken', 'image_link_pending_crawl', 'item_missing_required_attribute'}
sp_with_issues = []
all_disapproved_ids = set()

for row in ga_service.search(customer_id=customer_id, query='''
    SELECT shopping_product.item_id, shopping_product.title,
           shopping_product.status, shopping_product.issues
    FROM shopping_product
'''):
    p = row.shopping_product
    status = p.status.name
    if status in ('ELIGIBLE', 'ELIGIBLE_LIMITED', 'ACTIVE'):
        continue
    parts = p.item_id.split('_')
    shopify_id = int(parts[2]) if len(parts) >= 4 else None
    has_img_issue = any(i.error_code in IMG_ISSUES for i in p.issues)
    if not has_img_issue or not shopify_id:
        continue
    # Match to SP products
    for sp in all_sp:
        if sp['id'] == shopify_id:
            sp_with_issues.append(sp)
            print(f'  IMG ISSUE: {sp["title"]}')
            break

print(f'\nSP products with current image issues: {len(sp_with_issues)}')

# ===== STEP 5: Remove ALL Standard Process products from Google channel =====
print('\n' + '=' * 70)
print('STEP 5: Remove ALL SP products from Google & YouTube channel')
print('=' * 70)

if not google_pub_id:
    print('❌ SKIPPING — Could not find Google channel ID')
    print('You will need to do this manually in Shopify UI:')
    print('  1. Products → filter by Vendor "Standard Process Inc"')
    print('  2. Select all → Bulk edit')
    print('  3. Sales channels → uncheck Google & YouTube')
    print('  4. Save')
else:
    unpublish_mutation = """
    mutation publishableUnpublish($id: ID!, $input: [PublicationInput!]!) {
      publishableUnpublish(id: $id, input: $input) {
        userErrors { field message }
      }
    }
    """
    success = 0
    failed = []
    for sp in all_sp:
        product_gid = f'gid://shopify/Product/{sp["id"]}'
        r = requests.post(gql_url, headers=headers, json={
            'query': unpublish_mutation,
            'variables': {
                'id': product_gid,
                'input': [{'publicationId': google_pub_id}]
            }
        })
        data = r.json()
        errors = data.get('data', {}).get('publishableUnpublish', {}).get('userErrors', []) if data.get('data') else []
        if errors:
            failed.append((sp['title'], errors))
            print(f'  ❌ {sp["title"][:55]}: {errors}')
        else:
            success += 1
            print(f'  ✅ {sp["title"][:55]}')

    print(f'\nUnpublished: {success}/{len(all_sp)}')
    if failed:
        print(f'Failed: {len(failed)}')

print('\nDONE')
