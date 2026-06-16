#!/usr/bin/env python3
"""Fix account-default conversion goals: keep PURCHASE biddable, remove DOWNLOAD and BOOK_APPOINTMENT."""
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

# Find the resource names for DOWNLOAD and BOOK_APPOINTMENT customer conversion goals
ga_service = client.get_service('GoogleAdsService')

targets = []
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT customer_conversion_goal.resource_name,
           customer_conversion_goal.category,
           customer_conversion_goal.origin,
           customer_conversion_goal.biddable
    FROM customer_conversion_goal
    WHERE customer_conversion_goal.biddable = true
'''):
    g = row.customer_conversion_goal
    if g.category.name in ('DOWNLOAD', 'BOOK_APPOINTMENT'):
        targets.append((g.resource_name, g.category.name, g.origin.name))
        print(f'Found to demote: {g.category.name} / {g.origin.name} -> {g.resource_name}')

if not targets:
    print('Nothing to fix')
else:
    ccg_service = client.get_service('CustomerConversionGoalService')
    ops = []
    for rn, cat, origin in targets:
        op = client.get_type('CustomerConversionGoalOperation')
        op.update.resource_name = rn
        op.update.biddable = False
        op.update_mask.CopyFrom(protobuf_helpers.field_mask(None, op.update._pb))
        ops.append(op)

    try:
        response = ccg_service.mutate_customer_conversion_goals(customer_id=customer_id, operations=ops)
        for r in response.results:
            print(f'Updated: {r.resource_name}')
    except Exception as e:
        print(f'Failed: {e}')

# Verify
print('\n=== BIDDABLE ACCOUNT-DEFAULT GOALS (AFTER) ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT customer_conversion_goal.category, customer_conversion_goal.origin, customer_conversion_goal.biddable
    FROM customer_conversion_goal
    WHERE customer_conversion_goal.biddable = true
'''):
    g = row.customer_conversion_goal
    print(f'  Biddable: {g.category.name} / {g.origin.name}')
