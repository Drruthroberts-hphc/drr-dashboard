#!/usr/bin/env python3
"""1) Set Daily Wellness inventory to 999.
   2) Identify Standard Process products with image issues and remove from Google channel.
"""
import os
import requests
from dotenv import load_dotenv
load_dotenv()
from google.ads.googleads.client import GoogleAdsClient

# ===== SETUP =====
shop = os.getenv('SHOPIFY_STORE', 'drruthroberts-com.myshopify.com')
token = os.getenv('SHOPIFY_ACCESS_TOKEN')
api = f'https://{shop}/admin/api/2024-01'
gql_url = f'https://{shop}/admin/api/2024-01/graphql.json'
headers = {'X-Shopify-Access-Token': token, 'Content-Type': 'application/json'}

# Google Ads — get disapproved products
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

# ===== STEP A: Get ALL disapproved products from Google Ads =====
print('=' * 70)
print('STEP A: Identify ALL disapproved products + their issues')
print('=' * 70)
disapproved = []
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT shopping_product.item_id, shopping_product.title,
           shopping_product.status, shopping_product.issues
    FROM shopping_product
'''):
    p = row.shopping_product
    status = p.status.name if hasattr(p.status, 'name') else str(p.status)
    if status not in ('ELIGIBLE', 'ELIGIBLE_LIMITED', 'ACTIVE'):
        issues = [(i.error_code, i.description) for i in p.issues]
        # Extract Shopify product ID from item_id like 'shopify_US_6550287548481_39297408106561'
        parts = p.item_id.split('_')
        shopify_pid = parts[2] if len(parts) >= 4 else None
        disapproved.append({
            'item_id': p.item_id,
            'shopify_id': shopify_pid,
            'title': p.title,
            'issues': issues,
        })

# Filter to ONLY image-related issues
IMG_ISSUES = {'image_link_broken', 'image_link_pending_crawl', 'item_missing_required_attribute'}
image_issue_products = [
    d for d in disapproved
    if any(code in IMG_ISSUES for code, _ in d['issues'])
]
print(f'Total disapproved: {len(disapproved)}')
print(f'With image issues: {len(image_issue_products)}\n')

# ===== STEP B: For each image-issue product, look up Shopify vendor =====
print('=' * 70)
print('STEP B: Look up vendors via Shopify API')
print('=' * 70)
sp_products = []  # Standard Process products with image issues

for d in image_issue_products:
    if not d['shopify_id']:
        continue
    r = requests.get(f'{api}/products/{d["shopify_id"]}.json', headers=headers)
    if r.status_code != 200:
        print(f'  Could not fetch {d["title"][:50]} (status {r.status_code})')
        continue
    product = r.json().get('product', {})
    vendor = product.get('vendor', 'Unknown')
    handle = product.get('handle', '')
    print(f'  {vendor:30s} | {d["title"][:55]}')
    if vendor.strip().lower() == 'standard process':
        sp_products.append({
            'id': product['id'],
            'title': product['title'],
            'handle': handle,
            'vendor': vendor,
        })

print(f'\nStandard Process products with image issues: {len(sp_products)}')
for p in sp_products:
    print(f'  - {p["title"]} (ID {p["id"]})')

# ===== STEP C: Find Google & YouTube publication ID =====
print('\n' + '=' * 70)
print('STEP C: Find Google & YouTube sales channel ID')
print('=' * 70)
gql_query = """
{
  publications(first: 25) {
    edges { node { id name } }
  }
}
"""
r = requests.post(gql_url, headers=headers, json={'query': gql_query})
publications = r.json().get('data', {}).get('publications', {}).get('edges', [])
google_pub_id = None
for edge in publications:
    name = edge['node']['name']
    pid = edge['node']['id']
    print(f'  {name} -> {pid}')
    if 'google' in name.lower() and 'youtube' in name.lower():
        google_pub_id = pid

if not google_pub_id:
    # Try common variations
    for edge in publications:
        if 'google' in edge['node']['name'].lower():
            google_pub_id = edge['node']['id']
            print(f'Using: {edge["node"]["name"]}')
            break

print(f'\nGoogle channel ID: {google_pub_id}')

# ===== STEP D: Set Daily Wellness inventory to 999 =====
print('\n' + '=' * 70)
print('STEP D: Set Daily Wellness inventory to 999')
print('=' * 70)
# Find the product by searching for handle/title
r = requests.get(
    f'{api}/products.json',
    headers=headers,
    params={'handle': 'pets-daily-wellness-formula-for-pets-with-pork-beef-allergy'}
)
products = r.json().get('products', [])
if not products:
    # Try by title search
    r = requests.get(
        f'{api}/products.json',
        headers=headers,
        params={'title': "Pet's Daily Wellness Formula"}
    )
    products = r.json().get('products', [])

if products:
    dw = products[0]
    print(f'Found: {dw["title"]} (ID {dw["id"]})')
    for variant in dw.get('variants', []):
        print(f'  Variant: {variant["title"]} | Current qty: {variant.get("inventory_quantity")}')
        # Set inventory to 999 via inventory_levels endpoint
        # Need: location_id, inventory_item_id
        inv_item_id = variant.get('inventory_item_id')
        # Get inventory level locations
        r2 = requests.get(
            f'{api}/inventory_levels.json',
            headers=headers,
            params={'inventory_item_ids': inv_item_id}
        )
        levels = r2.json().get('inventory_levels', [])
        for level in levels:
            location_id = level.get('location_id')
            # Set to 999
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
                print(f'    Set qty=999 at location {location_id} ✅')
            else:
                print(f'    Failed at location {location_id}: {r3.status_code} - {r3.text[:200]}')
else:
    print('Could not find Daily Wellness product')

# ===== STEP E: Remove SP products from Google channel =====
print('\n' + '=' * 70)
print('STEP E: Remove Standard Process products from Google & YouTube channel')
print('=' * 70)

if not google_pub_id:
    print('SKIPPING — Google channel ID not found')
elif not sp_products:
    print('SKIPPING — No Standard Process products with image issues found')
else:
    unpublish_mutation = """
    mutation publishableUnpublish($id: ID!, $input: [PublicationInput!]!) {
      publishableUnpublish(id: $id, input: $input) {
        publishable { ... on Product { id title } }
        userErrors { field message }
      }
    }
    """
    for p in sp_products:
        product_gid = f'gid://shopify/Product/{p["id"]}'
        variables = {
            'id': product_gid,
            'input': [{'publicationId': google_pub_id}]
        }
        r = requests.post(gql_url, headers=headers, json={
            'query': unpublish_mutation,
            'variables': variables
        })
        result = r.json()
        if result.get('data', {}).get('publishableUnpublish', {}).get('userErrors'):
            errors = result['data']['publishableUnpublish']['userErrors']
            print(f'  ❌ {p["title"][:50]}: {errors}')
        else:
            print(f'  ✅ Unpublished from Google: {p["title"][:55]}')

print('\n' + '=' * 70)
print('DONE')
print('=' * 70)
