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

# Find any user lists with "shopify" in the name OR CRM_BASED type
print('=== SHOPIFY-RELATED USER LISTS ===\n')
shopify_lists = []
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT user_list.id, user_list.name, user_list.size_for_display,
           user_list.size_for_search, user_list.type, user_list.membership_status,
           user_list.crm_based_user_list.data_source_type
    FROM user_list
    WHERE user_list.type = 'CRM_BASED'
    ORDER BY user_list.name
'''):
    u = row.user_list
    name = u.name
    print(f'[{u.id}] {name[:55]}')
    print(f'    Size search: {u.size_for_search}  | Size display: {u.size_for_display}  | Status: {u.membership_status.name}')

# Specific check on the lists currently in the audience signal
print('\n=== LISTS IN CURRENT AUDIENCE SIGNAL (campaign 23781515935) ===\n')
audience_user_lists = []
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT asset_group_signal.audience.audience, asset_group.id
    FROM asset_group_signal
    WHERE asset_group.id = 6703896280
'''):
    audience_rn = row.asset_group_signal.audience.audience
    print(f'Audience: {audience_rn}')
    # Get the audience dimensions
    for arow in ga_service.search(customer_id=customer_id, query=f'''
        SELECT audience.name, audience.dimensions
        FROM audience
        WHERE audience.resource_name = "{audience_rn}"
    '''):
        a = arow.audience
        print(f'Name: {a.name}')
        for dim in a.dimensions:
            for seg in dim.audience_segments.segments:
                if seg.user_list.user_list:
                    list_rn = seg.user_list.user_list
                    list_id = list_rn.split('/')[-1]
                    audience_user_lists.append(list_id)

# Get details on each audience list
for list_id in audience_user_lists:
    for lrow in ga_service.search(customer_id=customer_id, query=f'''
        SELECT user_list.id, user_list.name, user_list.size_for_search,
               user_list.size_for_display, user_list.type, user_list.membership_status
        FROM user_list WHERE user_list.id = {list_id}
    '''):
        u = lrow.user_list
        print(f'  - [{u.id}] {u.name} | Search: {u.size_for_search} | Display: {u.size_for_display} | Type: {u.type.name}')
