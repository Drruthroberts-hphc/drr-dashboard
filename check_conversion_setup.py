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

# Campaign conversion goals
print('=== CAMPAIGN CONVERSION GOALS ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT campaign.id, campaign.name,
           campaign.selective_optimization.conversion_actions,
           campaign.optimization_goal_setting.optimization_goal_types
    FROM campaign WHERE campaign.id = 23781515935
'''):
    c = row.campaign
    if c.selective_optimization.conversion_actions:
        print(f'Selective optimization conversion actions: {list(c.selective_optimization.conversion_actions)}')
    else:
        print('Selective optimization: (not set — uses account-default)')

# Campaign-level conversion goal override
print('\n=== CAMPAIGN CONVERSION GOALS (detailed) ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT campaign_conversion_goal.category,
           campaign_conversion_goal.origin,
           campaign_conversion_goal.biddable
    FROM campaign_conversion_goal
    WHERE campaign.id = 23781515935
'''):
    g = row.campaign_conversion_goal
    print(f'Category: {g.category.name} | Origin: {g.origin.name} | Biddable: {g.biddable}')

# All conversion actions with Enhanced Conversions status
print('\n=== CONVERSION ACTIONS ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT conversion_action.id, conversion_action.name, conversion_action.status,
           conversion_action.primary_for_goal,
           conversion_action.category,
           conversion_action.counting_type,
           conversion_action.attribution_model_settings.attribution_model,
           conversion_action.click_through_lookback_window_days,
           conversion_action.view_through_lookback_window_days
    FROM conversion_action
    WHERE conversion_action.status != 'REMOVED'
    ORDER BY conversion_action.name
'''):
    a = row.conversion_action
    print(f'[{a.id}] {a.name} | Status: {a.status.name} | Primary: {a.primary_for_goal} | Category: {a.category.name}')
    print(f'    Counting: {a.counting_type.name} | Click window: {a.click_through_lookback_window_days}d | View window: {a.view_through_lookback_window_days}d')
    print(f'    Attribution: {a.attribution_model_settings.attribution_model.name}')

# Customer-level conversion goal configuration
print('\n=== CUSTOMER CONVERSION GOAL CATEGORIES ===')
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT customer_conversion_goal.category,
           customer_conversion_goal.origin,
           customer_conversion_goal.biddable
    FROM customer_conversion_goal
'''):
    g = row.customer_conversion_goal
    marker = '⭐ ACCOUNT-DEFAULT' if g.biddable else ''
    print(f'Category: {g.category.name} | Origin: {g.origin.name} | Biddable: {g.biddable} {marker}')
