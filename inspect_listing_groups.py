#!/usr/bin/env python3
import os
from dotenv import load_dotenv
load_dotenv()
from google.ads.googleads.client import GoogleAdsClient

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

query = '''
    SELECT asset_group_listing_group_filter.resource_name,
           asset_group_listing_group_filter.id,
           asset_group_listing_group_filter.type,
           asset_group_listing_group_filter.listing_source,
           asset_group_listing_group_filter.parent_listing_group_filter
    FROM asset_group_listing_group_filter
    WHERE asset_group.id = 6703896280
'''
print('=== CURRENT LISTING GROUP FILTERS ===')
rows = list(ga_service.search(customer_id=customer_id, query=query))
for row in rows:
    f = row.asset_group_listing_group_filter
    print(f'ID: {f.id} | Type: {f.type_.name} | Source: {f.listing_source.name} | Parent: {f.parent_listing_group_filter or "(root)"}')
print(f'Total: {len(rows)}')
