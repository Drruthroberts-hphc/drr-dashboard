#!/usr/bin/env python3
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

ca_service = client.get_service('ConversionActionService')
op = client.get_type('ConversionActionOperation')
op.update.resource_name = 'customers/6868436418/conversionActions/1037552323'
op.update.primary_for_goal = False
# Explicit field mask covering only this field
from google.protobuf import field_mask_pb2
op.update_mask.CopyFrom(field_mask_pb2.FieldMask(paths=['primary_for_goal']))

try:
    response = ca_service.mutate_conversion_actions(customer_id=customer_id, operations=[op])
    print(f'✅ Updated: {response.results[0].resource_name}')
except Exception as e:
    print(f'❌ Failed: {e}')

# Verify
ga_service = client.get_service('GoogleAdsService')
print('\n=== VERIFICATION ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT conversion_action.id, conversion_action.name, conversion_action.status,
           conversion_action.primary_for_goal, conversion_action.include_in_conversions_metric
    FROM conversion_action WHERE conversion_action.id = 1037552323
'''):
    a = row.conversion_action
    print(f'{a.name}: status={a.status.name} | primary_for_goal={a.primary_for_goal} | include_in_conv={a.include_in_conversions_metric}')
