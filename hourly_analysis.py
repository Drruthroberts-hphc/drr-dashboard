#!/usr/bin/env python3
import os
from dotenv import load_dotenv
load_dotenv()
from google.ads.googleads.client import GoogleAdsClient
from collections import defaultdict

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

# Hour of day analysis
print('=== BY HOUR OF DAY (Apr 21 - May 12) ===\n')
hours = defaultdict(lambda: {'spend': 0, 'conv': 0, 'value': 0, 'clicks': 0, 'impr': 0})
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT segments.hour, metrics.cost_micros, metrics.conversions,
           metrics.conversions_value, metrics.clicks, metrics.impressions
    FROM campaign
    WHERE campaign.id = 23781515935
      AND segments.date BETWEEN '2026-04-21' AND '2026-05-12'
'''):
    h = row.segments.hour
    hours[h]['spend'] += row.metrics.cost_micros / 1_000_000
    hours[h]['conv'] += row.metrics.conversions
    hours[h]['value'] += row.metrics.conversions_value
    hours[h]['clicks'] += row.metrics.clicks
    hours[h]['impr'] += row.metrics.impressions

print(f"{'Hour':<6}{'Spend':>10}{'Conv':>8}{'Value':>10}{'ROAS':>8}{'CPA':>8}{'Clicks':>8}")
print('-' * 60)
total_spend = total_conv = total_value = 0
for h in range(24):
    d = hours[h]
    roas = d['value'] / d['spend'] if d['spend'] > 0 else 0
    cpa = d['spend'] / d['conv'] if d['conv'] > 0 else 0
    total_spend += d['spend']
    total_conv += d['conv']
    total_value += d['value']
    flag = ' ⭐' if roas >= 5 else (' 🔴' if d['spend'] > 30 and roas < 2 else '')
    print(f"{h:02d}:00 {d['spend']:>9.2f} {d['conv']:>7.1f} {d['value']:>9.2f} {roas:>7.2f}x {cpa:>7.2f} {d['clicks']:>7}{flag}")
print('-' * 60)
overall_roas = total_value / total_spend if total_spend > 0 else 0
print(f'TOTAL  {total_spend:>9.2f} {total_conv:>7.1f} {total_value:>9.2f} {overall_roas:>7.2f}x')

# Day of week analysis
print('\n\n=== BY DAY OF WEEK ===\n')
DAY_NAMES = {1:'Sun', 2:'Mon', 3:'Tue', 4:'Wed', 5:'Thu', 6:'Fri', 7:'Sat'}
dow = defaultdict(lambda: {'spend': 0, 'conv': 0, 'value': 0, 'clicks': 0})
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT segments.day_of_week, metrics.cost_micros, metrics.conversions,
           metrics.conversions_value, metrics.clicks
    FROM campaign
    WHERE campaign.id = 23781515935
      AND segments.date BETWEEN '2026-04-21' AND '2026-05-12'
'''):
    d_num = row.segments.day_of_week
    # Google enum: MONDAY=2, TUESDAY=3, WEDNESDAY=4, THURSDAY=5, FRIDAY=6, SATURDAY=7, SUNDAY=8
    dow[d_num]['spend'] += row.metrics.cost_micros / 1_000_000
    dow[d_num]['conv'] += row.metrics.conversions
    dow[d_num]['value'] += row.metrics.conversions_value
    dow[d_num]['clicks'] += row.metrics.clicks

# Google enum values
DAY_ENUM = {2:'Mon', 3:'Tue', 4:'Wed', 5:'Thu', 6:'Fri', 7:'Sat', 8:'Sun'}
print(f"{'Day':<6}{'Spend':>10}{'Conv':>8}{'Value':>10}{'ROAS':>8}{'CPA':>8}{'Clicks':>8}")
print('-' * 60)
for d_num in [2,3,4,5,6,7,8]:
    d = dow[d_num]
    roas = d['value'] / d['spend'] if d['spend'] > 0 else 0
    cpa = d['spend'] / d['conv'] if d['conv'] > 0 else 0
    print(f"{DAY_ENUM[d_num]:<6}{d['spend']:>9.2f} {d['conv']:>7.1f} {d['value']:>9.2f} {roas:>7.2f}x {cpa:>7.2f} {d['clicks']:>7}")
