#!/usr/bin/env python3
"""Last week status report (June 1-7, 2026)."""
import os
from collections import defaultdict
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

# Last week: June 1-7
# Previous week: May 25-31
def pull_week(start, end, label):
    spend = conv = value = clicks = impr = 0
    daily = []
    for row in ga_service.search(customer_id=customer_id, query=f'''
        SELECT segments.date,
               metrics.cost_micros, metrics.conversions, metrics.conversions_value,
               metrics.clicks, metrics.impressions
        FROM campaign
        WHERE campaign.id = 23781515935
          AND segments.date BETWEEN '{start}' AND '{end}'
        ORDER BY segments.date ASC
    '''):
        m = row.metrics
        s = m.cost_micros / 1_000_000
        spend += s
        conv += m.conversions
        value += m.conversions_value
        clicks += m.clicks
        impr += m.impressions
        daily.append({
            'date': str(row.segments.date),
            'spend': s, 'conv': m.conversions, 'value': m.conversions_value,
            'clicks': m.clicks, 'impr': m.impressions,
        })
    roas = value / spend if spend > 0 else 0
    cpa = spend / conv if conv > 0 else 0
    conv_rate = conv / clicks if clicks > 0 else 0
    return {
        'label': label, 'spend': spend, 'conv': conv, 'value': value,
        'roas': roas, 'cpa': cpa, 'conv_rate': conv_rate,
        'clicks': clicks, 'impr': impr, 'daily': daily,
    }

last_week = pull_week('2026-06-01', '2026-06-07', 'Last Week (Jun 1-7)')
prev_week = pull_week('2026-05-25', '2026-05-31', 'Prev Week (May 25-31)')

print('=' * 70)
print(f'LAST WEEK: {last_week["label"]}')
print('=' * 70)
for d in last_week['daily']:
    r = d['value'] / d['spend'] if d['spend'] > 0 else 0
    print(f"  {d['date']} | Spend: ${d['spend']:>7.2f} | Conv: {d['conv']:>5.2f} | Value: ${d['value']:>8.2f} | ROAS: {r:>5.2f}x")
print(f"\n  TOTAL: ${last_week['spend']:.2f} | Conv: {last_week['conv']:.1f} | Value: ${last_week['value']:.2f}")
print(f"  ROAS: {last_week['roas']:.2f}x | CPA: ${last_week['cpa']:.2f} | Conv Rate: {last_week['conv_rate']*100:.2f}%")
print(f"  Clicks: {last_week['clicks']} | Impressions: {last_week['impr']}")

print('\n' + '=' * 70)
print(f'PREVIOUS WEEK: {prev_week["label"]}')
print('=' * 70)
for d in prev_week['daily']:
    r = d['value'] / d['spend'] if d['spend'] > 0 else 0
    print(f"  {d['date']} | Spend: ${d['spend']:>7.2f} | Conv: {d['conv']:>5.2f} | Value: ${d['value']:>8.2f} | ROAS: {r:>5.2f}x")
print(f"\n  TOTAL: ${prev_week['spend']:.2f} | Conv: {prev_week['conv']:.1f} | Value: ${prev_week['value']:.2f}")
print(f"  ROAS: {prev_week['roas']:.2f}x | CPA: ${prev_week['cpa']:.2f} | Conv Rate: {prev_week['conv_rate']*100:.2f}%")
print(f"  Clicks: {prev_week['clicks']} | Impressions: {prev_week['impr']}")

# Feed Health
print('\n' + '=' * 70)
print('MERCHANT CENTER FEED HEALTH (current)')
print('=' * 70)
status_counter = defaultdict(int)
disapproved = []
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT shopping_product.item_id, shopping_product.title,
           shopping_product.status, shopping_product.issues
    FROM shopping_product
'''):
    p = row.shopping_product
    s = p.status.name
    status_counter[s] += 1
    if s not in ('ELIGIBLE', 'ELIGIBLE_LIMITED', 'ACTIVE'):
        issues = [(i.error_code, i.description) for i in p.issues]
        disapproved.append({'title': p.title, 'item_id': p.item_id, 'issues': issues})

print(f'Total products: {sum(status_counter.values())}')
for s, c in status_counter.items():
    print(f'  {s}: {c}')

print(f'\nDisapproved details:')
for d in disapproved:
    issue_codes = [code for code, _ in d['issues']]
    print(f'  • {d["title"][:60]}')
    for code, desc in d['issues']:
        print(f'      - {code}: {desc[:80]}')
