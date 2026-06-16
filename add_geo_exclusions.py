#!/usr/bin/env python3
"""Exclude North Carolina and San Francisco Bay Area from the PMax campaign."""
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
campaign_rn = 'customers/6868436418/campaigns/23781515935'
ga_service = client.get_service('GoogleAdsService')

# Look up Geo Target Constant IDs for North Carolina and San Francisco Bay Area
print('=== Looking up geo target IDs ===\n')
geo_ids_to_exclude = {}

# Search for North Carolina (state in US)
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT geo_target_constant.resource_name, geo_target_constant.canonical_name,
           geo_target_constant.id, geo_target_constant.target_type
    FROM geo_target_constant
    WHERE geo_target_constant.name = 'North Carolina'
      AND geo_target_constant.country_code = 'US'
      AND geo_target_constant.target_type = 'State'
      AND geo_target_constant.status = 'ENABLED'
'''):
    g = row.geo_target_constant
    print(f'Found: {g.canonical_name} -> {g.resource_name} (ID {g.id})')
    geo_ids_to_exclude['North Carolina'] = g.resource_name

# Search for San Francisco Bay Area
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT geo_target_constant.resource_name, geo_target_constant.canonical_name,
           geo_target_constant.id, geo_target_constant.target_type
    FROM geo_target_constant
    WHERE geo_target_constant.name = 'San Francisco Bay Area'
      AND geo_target_constant.country_code = 'US'
      AND geo_target_constant.status = 'ENABLED'
'''):
    g = row.geo_target_constant
    print(f'Found: {g.canonical_name} -> {g.resource_name} (ID {g.id})')
    geo_ids_to_exclude['SF Bay Area'] = g.resource_name

if not geo_ids_to_exclude:
    print('ERROR: No geo target IDs found')
else:
    print(f'\n=== Adding {len(geo_ids_to_exclude)} negative location criteria ===\n')
    cc_service = client.get_service('CampaignCriterionService')
    ops = []
    for name, geo_rn in geo_ids_to_exclude.items():
        op = client.get_type('CampaignCriterionOperation')
        op.create.campaign = campaign_rn
        op.create.location.geo_target_constant = geo_rn
        op.create.negative = True  # This is the key — makes it EXCLUDE
        ops.append(op)

    try:
        response = cc_service.mutate_campaign_criteria(customer_id=customer_id, operations=ops)
        for r in response.results:
            print(f'✅ Created exclusion: {r.resource_name}')
    except Exception as e:
        print(f'❌ Failed: {e}')
        raise

# Verify
print('\n=== VERIFICATION: All location criteria for campaign ===\n')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT campaign_criterion.criterion_id, campaign_criterion.negative,
           campaign_criterion.location.geo_target_constant,
           campaign_criterion.status
    FROM campaign_criterion
    WHERE campaign.id = 23781515935
      AND campaign_criterion.type = 'LOCATION'
    ORDER BY campaign_criterion.negative
'''):
    c = row.campaign_criterion
    excl = '🚫 NEGATIVE' if c.negative else '✅ INCLUDE'
    print(f'{excl}: {c.location.geo_target_constant} [{c.status.name}]')
