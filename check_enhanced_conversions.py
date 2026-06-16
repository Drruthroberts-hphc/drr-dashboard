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

# Check Conversion Action's Enhanced Conversion Settings for Google Shopping App Purchase
print('=== GOOGLE SHOPPING APP PURCHASE — ENHANCED CONVERSIONS ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT conversion_action.id, conversion_action.name, conversion_action.status,
           conversion_action.primary_for_goal,
           conversion_action.type,
           conversion_action.category,
           conversion_action.include_in_conversions_metric
    FROM conversion_action
    WHERE conversion_action.id = 435221278
'''):
    a = row.conversion_action
    print(f'Name: {a.name}')
    print(f'Status: {a.status.name}')
    print(f'Type: {a.type_.name}')
    print(f'Category: {a.category.name}')
    print(f'Primary for goal: {a.primary_for_goal}')
    print(f'Include in conversions: {a.include_in_conversions_metric}')

# Check customer-level Conversion tracking settings
print('\n=== CUSTOMER CONVERSION TRACKING SETTINGS ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT customer.id, customer.conversion_tracking_setting.conversion_tracking_id,
           customer.conversion_tracking_setting.google_ads_conversion_customer,
           customer.conversion_tracking_setting.conversion_tracking_status,
           customer.conversion_tracking_setting.accepted_customer_data_terms,
           customer.conversion_tracking_setting.enhanced_conversions_for_leads_enabled
    FROM customer
'''):
    s = row.customer.conversion_tracking_setting
    print(f'Tracking ID: {s.conversion_tracking_id}')
    print(f'Google Ads conversion customer: {s.google_ads_conversion_customer}')
    print(f'Tracking status: {s.conversion_tracking_status.name}')
    print(f'✅ Accepted customer data terms (required for Enhanced Conversions): {s.accepted_customer_data_terms}')
    print(f'Enhanced conversions for LEADS enabled: {s.enhanced_conversions_for_leads_enabled}')
