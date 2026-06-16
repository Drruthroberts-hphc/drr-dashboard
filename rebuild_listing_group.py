#!/usr/bin/env python3
"""Rebuild listing group with proper PMax tree structure:
   Root SUBDIVISION → Child UNIT_INCLUDED (all products).
"""
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
ga_service = client.get_service('GoogleAdsService')

# Step 1: Find and remove existing listing group filters for this asset group
existing = list(ga_service.search(
    customer_id=customer_id,
    query='SELECT asset_group_listing_group_filter.resource_name FROM asset_group_listing_group_filter WHERE asset_group.id = 6703896280'
))

if existing:
    print(f'Removing {len(existing)} existing listing group(s)...')
    remove_ops = []
    for row in existing:
        op = client.get_type('AssetGroupListingGroupFilterOperation')
        op.remove = row.asset_group_listing_group_filter.resource_name
        remove_ops.append(op)
    try:
        resp = alg_service.mutate_asset_group_listing_group_filters(customer_id=customer_id, operations=remove_ops)
        print(f'  Removed {len(resp.results)}')
    except Exception as e:
        print(f'  Remove error (continuing): {e}')

# Step 2: Create proper tree — SUBDIVISION root, then UNIT_INCLUDED child
# Use temp resource names with negative IDs
TEMP_ROOT = 'customers/6868436418/assetGroupListingGroupFilters/6703896280~-1'

# Root node: SUBDIVISION, no case_value, no parent
op_root = client.get_type('AssetGroupListingGroupFilterOperation')
op_root.create.resource_name = TEMP_ROOT
op_root.create.asset_group = asset_group_rn
op_root.create.type_ = client.enums.ListingGroupFilterTypeEnum.SUBDIVISION
op_root.create.listing_source = client.enums.ListingGroupFilterListingSourceEnum.SHOPPING
# no parent, no case_value = root

# Child node: UNIT_INCLUDED, no case_value, parent = root (all products)
op_child = client.get_type('AssetGroupListingGroupFilterOperation')
op_child.create.asset_group = asset_group_rn
op_child.create.type_ = client.enums.ListingGroupFilterTypeEnum.UNIT_INCLUDED
op_child.create.listing_source = client.enums.ListingGroupFilterListingSourceEnum.SHOPPING
op_child.create.parent_listing_group_filter = TEMP_ROOT
# no case_value on the only child = "Other" / catch-all = all products

try:
    response = alg_service.mutate_asset_group_listing_group_filters(
        customer_id=customer_id,
        operations=[op_root, op_child]
    )
    for r in response.results:
        print(f'Created: {r.resource_name}')
except Exception as e:
    print(f'Failed: {e}')
    raise

# Verify
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
