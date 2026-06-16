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

# Campaign shopping settings
query = '''
    SELECT campaign.id, campaign.name, campaign.status,
           campaign.shopping_setting.merchant_id,
           campaign.shopping_setting.feed_label,
           campaign.shopping_setting.campaign_priority,
           campaign.shopping_setting.enable_local
    FROM campaign
    WHERE campaign.id = 23781515935
'''
print('=== CAMPAIGN SHOPPING SETTINGS ===')
for row in ga_service.search(customer_id=customer_id, query=query):
    c = row.campaign
    s = c.shopping_setting
    print(f'Name: {c.name}')
    print(f'Status: {c.status.name}')
    print(f'Merchant ID: {s.merchant_id}')
    print(f'Feed label: "{s.feed_label}"')
    print(f'Priority: {s.campaign_priority}')
    print(f'Enable local: {s.enable_local}')
