#!/usr/bin/env python3
"""Find which conversion action has label 'c92YCKP8qOYDEMOXyc0D' in tag snippets."""
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

print('Searching all conversion actions for label "c92YCKP8qOYDEMOXyc0D"...\n')
TARGET = 'c92YCKP8qOYDEMOXyc0D'
found = False

for row in ga_service.search(customer_id=customer_id, query='''
    SELECT conversion_action.id, conversion_action.name, conversion_action.status,
           conversion_action.tag_snippets, conversion_action.primary_for_goal,
           conversion_action.include_in_conversions_metric, conversion_action.category
    FROM conversion_action
'''):
    a = row.conversion_action
    for snippet in a.tag_snippets:
        global_site = snippet.global_site_tag or ''
        event = snippet.event_snippet or ''
        combined = global_site + event
        if TARGET in combined:
            found = True
            print(f'🎯 MATCH FOUND')
            print(f'   ID: {a.id}')
            print(f'   Name: {a.name}')
            print(f'   Status: {a.status.name}')
            print(f'   Category: {a.category.name}')
            print(f'   Primary for goal: {a.primary_for_goal}')
            print(f'   Include in conversions: {a.include_in_conversions_metric}')
            print()
            break

if not found:
    print('No conversion action found with that label in tag snippets.')
    print('It may be from a deleted conversion or external source.')
