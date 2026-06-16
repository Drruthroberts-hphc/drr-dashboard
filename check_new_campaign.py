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

print('=== Sales-PMax-2026-04-Relaunch — DAILY PERFORMANCE ===\n')
total_spend = 0
total_conv = 0
total_value = 0
total_clicks = 0
total_impr = 0
days = 0
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT segments.date,
           metrics.cost_micros, metrics.conversions, metrics.conversions_value,
           metrics.clicks, metrics.impressions, metrics.ctr
    FROM campaign
    WHERE campaign.id = 23781515935
      AND segments.date BETWEEN '2026-04-21' AND '2026-04-27'
    ORDER BY segments.date DESC
'''):
    m = row.metrics
    spend = m.cost_micros / 1_000_000
    roas = (m.conversions_value / spend) if spend > 0 else 0
    cpa = (spend / m.conversions) if m.conversions > 0 else 0
    print(f'{row.segments.date} | Spend: ${spend:>7.2f} | Conv: {m.conversions:>5.2f} | Value: ${m.conversions_value:>8.2f} | ROAS: {roas:>5.2f}x | CPA: ${cpa:>6.2f} | Clicks: {m.clicks:>4} | Impr: {m.impressions:>5}')
    total_spend += spend
    total_conv += m.conversions
    total_value += m.conversions_value
    total_clicks += m.clicks
    total_impr += m.impressions
    days += 1

print(f'\n--- TOTAL ({days} days) ---')
total_roas = (total_value / total_spend) if total_spend > 0 else 0
total_cpa = (total_spend / total_conv) if total_conv > 0 else 0
print(f'Spend: ${total_spend:.2f} | Conv: {total_conv:.2f} | Value: ${total_value:.2f} | ROAS: {total_roas:.2f}x | CPA: ${total_cpa:.2f}')
print(f'Clicks: {total_clicks} | Impressions: {total_impr}')

# Budget pacing
print(f'\nDaily budget: $75 — pacing at ${total_spend/max(days,1):.2f}/day avg')
