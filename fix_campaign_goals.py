#!/usr/bin/env python3
"""Set campaign-level conversion goals: only PURCHASE biddable, all others not."""
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
ga_service = client.get_service('GoogleAdsService')

# List all campaign conversion goals for this campaign
print('=== CAMPAIGN CONVERSION GOALS BEFORE ===')
targets = []
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT campaign_conversion_goal.resource_name,
           campaign_conversion_goal.category,
           campaign_conversion_goal.origin,
           campaign_conversion_goal.biddable
    FROM campaign_conversion_goal
    WHERE campaign.id = 23781515935
'''):
    g = row.campaign_conversion_goal
    print(f'{g.category.name} / {g.origin.name} / Biddable: {g.biddable} -> {g.resource_name}')
    # Flag anything biddable that isn't PURCHASE/WEBSITE
    if g.biddable and not (g.category.name == 'PURCHASE' and g.origin.name == 'WEBSITE'):
        targets.append(g.resource_name)

print(f'\nTargets to demote: {len(targets)}')

# Demote them
if targets:
    ccg_service = client.get_service('CampaignConversionGoalService')
    ops = []
    for rn in targets:
        op = client.get_type('CampaignConversionGoalOperation')
        op.update.resource_name = rn
        op.update.biddable = False
        op.update_mask.CopyFrom(protobuf_helpers.field_mask(None, op.update._pb))
        ops.append(op)
    try:
        response = ccg_service.mutate_campaign_conversion_goals(customer_id=customer_id, operations=ops)
        for r in response.results:
            print(f'Updated: {r.resource_name}')
    except Exception as e:
        print(f'Failed: {e}')

# Verify
print('\n=== CAMPAIGN CONVERSION GOALS AFTER ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT campaign_conversion_goal.category,
           campaign_conversion_goal.origin,
           campaign_conversion_goal.biddable
    FROM campaign_conversion_goal
    WHERE campaign.id = 23781515935 AND campaign_conversion_goal.biddable = true
'''):
    g = row.campaign_conversion_goal
    print(f'BIDDABLE: {g.category.name} / {g.origin.name}')
