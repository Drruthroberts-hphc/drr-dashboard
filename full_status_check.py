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

# Campaign status
print('=== CAMPAIGN ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT campaign.id, campaign.name, campaign.status, campaign.serving_status,
           campaign.primary_status, campaign.primary_status_reasons
    FROM campaign WHERE campaign.id = 23781515935
'''):
    c = row.campaign
    reasons = [r.name for r in c.primary_status_reasons] if c.primary_status_reasons else []
    print(f'Status: {c.status.name} | Serving: {c.serving_status.name} | Primary: {c.primary_status.name}')
    if reasons:
        print(f'Reasons: {reasons}')

# Asset group status
print('\n=== ASSET GROUP ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT asset_group.id, asset_group.name, asset_group.status,
           asset_group.primary_status, asset_group.primary_status_reasons
    FROM asset_group WHERE asset_group.id = 6703896280
'''):
    ag = row.asset_group
    reasons = [r.name for r in ag.primary_status_reasons] if ag.primary_status_reasons else []
    print(f'Name: {ag.name}')
    print(f'Status: {ag.status.name} | Primary: {ag.primary_status.name}')
    if reasons:
        print(f'Reasons: {reasons}')

# Asset group assets with status
print('\n=== ASSET GROUP ASSETS (by field type + status) ===')
ag_assets = {}
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT asset_group_asset.field_type, asset_group_asset.status,
           asset.id, asset.name, asset.type
    FROM asset_group_asset
    WHERE asset_group.id = 6703896280
'''):
    ft = row.asset_group_asset.field_type.name
    status = row.asset_group_asset.status.name
    key = f'{ft} / {status}'
    ag_assets[key] = ag_assets.get(key, 0) + 1
for k, v in sorted(ag_assets.items()):
    print(f'  {v}x  {k}')

# Campaign assets
print('\n=== CAMPAIGN ASSETS (by field type + status) ===')
c_assets = {}
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT campaign_asset.field_type, campaign_asset.status,
           asset.id, asset.type
    FROM campaign_asset WHERE campaign.id = 23781515935
'''):
    ft = row.campaign_asset.field_type.name
    status = row.campaign_asset.status.name
    key = f'{ft} / {status}'
    c_assets[key] = c_assets.get(key, 0) + 1
for k, v in sorted(c_assets.items()):
    print(f'  {v}x  {k}')

# Check for PAUSED or REMOVED anywhere
print('\n=== SCANNING FOR PAUSED/REMOVED ===')
found = False
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT asset_group_asset.field_type, asset_group_asset.status, asset.id, asset.name
    FROM asset_group_asset
    WHERE asset_group.id = 6703896280
      AND asset_group_asset.status != 'ENABLED'
'''):
    a = row
    print(f'AG Asset not enabled: {a.asset_group_asset.field_type.name} / {a.asset_group_asset.status.name} / asset {a.asset.id}')
    found = True
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT campaign_asset.field_type, campaign_asset.status, asset.id
    FROM campaign_asset
    WHERE campaign.id = 23781515935
      AND campaign_asset.status != 'ENABLED'
'''):
    a = row
    print(f'Campaign Asset not enabled: {a.campaign_asset.field_type.name} / {a.campaign_asset.status.name} / asset {a.asset.id}')
    found = True
if not found:
    print('All assets ENABLED ✓')
