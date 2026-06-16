#!/usr/bin/env python3
import os
from dotenv import load_dotenv
load_dotenv()
from google.ads.googleads.client import GoogleAdsClient
from collections import Counter

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

print('=== SHOPPING PRODUCT STATUS ===\n')

# Try shopping_product table
status_counter = Counter()
issue_counter = Counter()
disapproved_products = []
all_products = []

for row in ga_service.search(customer_id=customer_id, query='''
    SELECT shopping_product.item_id, shopping_product.title,
           shopping_product.status, shopping_product.issues
    FROM shopping_product
'''):
    p = row.shopping_product
    status = p.status.name if hasattr(p.status, 'name') else str(p.status)
    status_counter[status] += 1
    all_products.append((p.item_id, p.title, status))
    if status not in ('ELIGIBLE', 'ELIGIBLE_LIMITED', 'ACTIVE'):
        disapproved_products.append({
            'item_id': p.item_id,
            'title': p.title,
            'status': status,
            'issues': [(i.error_code, i.description) for i in p.issues]
        })
        for issue in p.issues:
            issue_counter[issue.error_code] += 1

print('STATUS BREAKDOWN:')
for s, c in status_counter.most_common():
    print(f'  {c}x  {s}')

print(f'\nTotal products: {sum(status_counter.values())}')
print(f'Disapproved/limited: {len(disapproved_products)}')

if disapproved_products:
    print('\n=== DISAPPROVED/LIMITED PRODUCTS ===\n')
    for p in disapproved_products:
        print(f'• {p["title"][:60]}')
        print(f'    Item ID: {p["item_id"]}')
        print(f'    Status: {p["status"]}')
        for code, desc in p['issues']:
            print(f'    Issue: {code} — {desc}')
        print()

if issue_counter:
    print('=== ISSUE FREQUENCY ===\n')
    for code, count in issue_counter.most_common():
        print(f'  {count}x  {code}')
