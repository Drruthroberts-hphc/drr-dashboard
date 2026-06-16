#!/usr/bin/env python3
"""Create a single UNIT_INCLUDED root listing group (all products, no filter)."""
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
asset_group_rn = 'customers/6868436418/assetGroups/6703896280'

alg_service = client.get_service('AssetGroupListingGroupFilterService')

# Single UNIT_INCLUDED root, no case_value, no parent = "all products"
op = client.get_type('AssetGroupListingGroupFilterOperation')
op.create.asset_group = asset_group_rn
op.create.type_ = client.enums.ListingGroupFilterTypeEnum.UNIT_INCLUDED
op.create.listing_source = client.enums.ListingGroupFilterListingSourceEnum.SHOPPING

response = alg_service.mutate_asset_group_listing_group_filters(
    customer_id=customer_id,
    operations=[op]
)
for r in response.results:
    print(f'Created: {r.resource_name}')

# Verify
ga_service = client.get_service('GoogleAdsService')
print('\n=== VERIFICATION ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT asset_group_listing_group_filter.id,
           asset_group_listing_group_filter.type,
           asset_group_listing_group_filter.listing_source,
           asset_group_listing_group_filter.parent_listing_group_filter
    FROM asset_group_listing_group_filter
    WHERE asset_group.id = 6703896280
'''):
    f = row.asset_group_listing_group_filter
    print(f'ID: {f.id} | Type: {f.type_.name} | Source: {f.listing_source.name} | Parent: {f.parent_listing_group_filter or "(ROOT)"}')
