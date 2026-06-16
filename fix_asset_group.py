#!/usr/bin/env python3
"""Enable asset group and add Shopping listing group."""
import os
from dotenv import load_dotenv
load_dotenv()
from google.ads.googleads.client import GoogleAdsClient
from google.api_core import protobuf_helpers

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

# Step 1: Enable the asset group
ag_service = client.get_service('AssetGroupService')
op = client.get_type('AssetGroupOperation')
op.update.resource_name = asset_group_rn
op.update.status = client.enums.AssetGroupStatusEnum.ENABLED
op.update_mask.CopyFrom(protobuf_helpers.field_mask(None, op.update._pb))

try:
    response = ag_service.mutate_asset_groups(customer_id=customer_id, operations=[op])
    print(f'Asset group ENABLED: {response.results[0].resource_name}')
except Exception as e:
    print(f'Asset group enable failed: {e}')

# Step 2: Create a listing group (default "all products") for Shopping
alg_service = client.get_service('AssetGroupListingGroupFilterService')
op2 = client.get_type('AssetGroupListingGroupFilterOperation')
op2.create.asset_group = asset_group_rn
op2.create.type_ = client.enums.ListingGroupFilterTypeEnum.UNIT_INCLUDED
op2.create.listing_source = client.enums.ListingGroupFilterListingSourceEnum.SHOPPING

try:
    response = alg_service.mutate_asset_group_listing_group_filters(customer_id=customer_id, operations=[op2])
    print(f'Listing group created: {response.results[0].resource_name}')
except Exception as e:
    print(f'Listing group creation failed: {e}')

# Verify
ga_service = client.get_service('GoogleAdsService')
print('\n=== VERIFICATION ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT asset_group.id, asset_group.name, asset_group.status
    FROM asset_group WHERE asset_group.id = 6703896280
'''):
    print(f'Asset group: {row.asset_group.name} | Status: {row.asset_group.status.name}')

for row in ga_service.search(customer_id=customer_id, query='''
    SELECT asset_group_listing_group_filter.resource_name, asset_group_listing_group_filter.type,
           asset_group_listing_group_filter.listing_source
    FROM asset_group_listing_group_filter
    WHERE asset_group.id = 6703896280
'''):
    f = row.asset_group_listing_group_filter
    print(f'Listing group: {f.resource_name} | Type: {f.type_.name} | Source: {f.listing_source.name}')
