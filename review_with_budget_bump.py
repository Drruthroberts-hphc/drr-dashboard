#!/usr/bin/env python3
import os
from datetime import date
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

today = date.today().isoformat()
print(f'Today: {today}\n')
print(f'=== Sales-PMax-2026-04-Relaunch — Apr 21 to {today} ===\n')

# Pull all data
rows_data = []
for row in ga_service.search(customer_id=customer_id, query=f'''
    SELECT segments.date,
           metrics.cost_micros, metrics.conversions, metrics.conversions_value,
           metrics.clicks, metrics.impressions
    FROM campaign
    WHERE campaign.id = 23781515935
      AND segments.date BETWEEN '2026-04-21' AND '{today}'
    ORDER BY segments.date ASC
'''):
    m = row.metrics
    spend = m.cost_micros / 1_000_000
    roas = (m.conversions_value / spend) if spend > 0 else 0
    rows_data.append({
        'date': str(row.segments.date),
        'spend': spend,
        'conv': m.conversions,
        'value': m.conversions_value,
        'roas': roas,
        'clicks': m.clicks,
        'impr': m.impressions
    })

# Print all daily
for r in rows_data:
    marker = ' 💰' if r['date'] >= '2026-04-30' else ''
    print(f"{r['date']} | Spend: ${r['spend']:>7.2f} | Conv: {r['conv']:>5.2f} | Value: ${r['value']:>8.2f} | ROAS: {r['roas']:>5.2f}x | Clicks: {r['clicks']:>4} | Impr: {r['impr']:>5}{marker}")

# Pre vs post budget change (Apr 30)
pre = [r for r in rows_data if r['date'] < '2026-04-30']
post = [r for r in rows_data if r['date'] >= '2026-04-30']

def aggregate(rows, label):
    if not rows:
        return
    total_spend = sum(r['spend'] for r in rows)
    total_conv = sum(r['conv'] for r in rows)
    total_value = sum(r['value'] for r in rows)
    total_clicks = sum(r['clicks'] for r in rows)
    total_impr = sum(r['impr'] for r in rows)
    days = len(rows)
    roas = total_value / total_spend if total_spend > 0 else 0
    cpa = total_spend / total_conv if total_conv > 0 else 0
    avg_daily_spend = total_spend / days
    print(f'\n--- {label} ({days} days) ---')
    print(f'Total Spend: ${total_spend:.2f}  | Avg/day: ${avg_daily_spend:.2f}')
    print(f'Total Conv: {total_conv:.2f}    | Conv Value: ${total_value:.2f}')
    print(f'ROAS: {roas:.2f}x | CPA: ${cpa:.2f}')
    print(f'Clicks: {total_clicks}  | Impressions: {total_impr}')

aggregate(pre, 'PRE budget bump (Apr 21-29, $75/day)')
aggregate(post, 'POST budget bump (Apr 30-today, $100/day)')
aggregate(rows_data, 'OVERALL since launch')
