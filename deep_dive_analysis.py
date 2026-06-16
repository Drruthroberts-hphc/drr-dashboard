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

DATE_FROM = '2026-04-21'
DATE_TO = '2026-05-12'

# ====== 1. PRODUCT-LEVEL PERFORMANCE (Shopping) ======
print('=== PRODUCT-LEVEL PERFORMANCE (Shopping inside PMax) ===\n')
products = defaultdict(lambda: {'spend': 0, 'conv': 0, 'value': 0, 'clicks': 0, 'impr': 0, 'title': ''})
try:
    for row in ga_service.search(customer_id=customer_id, query=f'''
        SELECT campaign.id, segments.product_item_id, segments.product_title,
               metrics.cost_micros, metrics.conversions, metrics.conversions_value,
               metrics.clicks, metrics.impressions
        FROM shopping_performance_view
        WHERE campaign.id = 23781515935
          AND segments.date BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
    '''):
        pid = row.segments.product_item_id or 'UNKNOWN'
        products[pid]['title'] = row.segments.product_title or pid
        products[pid]['spend'] += row.metrics.cost_micros / 1_000_000
        products[pid]['conv'] += row.metrics.conversions
        products[pid]['value'] += row.metrics.conversions_value
        products[pid]['clicks'] += row.metrics.clicks
        products[pid]['impr'] += row.metrics.impressions
except Exception as e:
    print(f'shopping_performance_view failed: {e}\n')

if products:
    # Sort by spend desc
    sorted_products = sorted(products.items(), key=lambda x: -x[1]['spend'])
    print(f"{'Product':<50}{'Spend':>10}{'Conv':>7}{'Value':>10}{'ROAS':>7}{'Impr':>8}")
    print('-' * 95)
    for pid, d in sorted_products[:30]:
        roas = d['value'] / d['spend'] if d['spend'] > 0 else 0
        title = d['title'][:48]
        print(f"{title:<50}{d['spend']:>9.2f} {d['conv']:>6.1f} {d['value']:>9.2f} {roas:>6.2f}x {d['impr']:>7}")
    print(f'\nTotal products with activity: {len(products)}')

# ====== 2. GEOGRAPHIC PERFORMANCE ======
print('\n\n=== GEOGRAPHIC PERFORMANCE (Top 15 states) ===\n')
geos = defaultdict(lambda: {'spend': 0, 'conv': 0, 'value': 0, 'name': ''})
try:
    for row in ga_service.search(customer_id=customer_id, query=f'''
        SELECT campaign.id, segments.geo_target_region,
               metrics.cost_micros, metrics.conversions, metrics.conversions_value
        FROM geographic_view
        WHERE campaign.id = 23781515935
          AND segments.date BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
    '''):
        region_rn = row.segments.geo_target_region
        geos[region_rn]['spend'] += row.metrics.cost_micros / 1_000_000
        geos[region_rn]['conv'] += row.metrics.conversions
        geos[region_rn]['value'] += row.metrics.conversions_value

    # Resolve region names
    region_rns = [rn for rn in geos.keys() if rn]
    if region_rns:
        for i in range(0, len(region_rns), 50):
            batch = region_rns[i:i+50]
            in_list = ','.join(f'"{r}"' for r in batch)
            for row in ga_service.search(customer_id=customer_id, query=f'''
                SELECT geo_target_constant.resource_name, geo_target_constant.canonical_name,
                       geo_target_constant.country_code, geo_target_constant.target_type
                FROM geo_target_constant
                WHERE geo_target_constant.resource_name IN ({in_list})
            '''):
                rn = row.geo_target_constant.resource_name
                if rn in geos:
                    geos[rn]['name'] = row.geo_target_constant.canonical_name
except Exception as e:
    print(f'geographic_view failed: {e}\n')

if geos:
    sorted_geos = sorted(geos.items(), key=lambda x: -x[1]['spend'])
    print(f"{'Location':<50}{'Spend':>10}{'Conv':>7}{'Value':>10}{'ROAS':>7}")
    print('-' * 85)
    for rn, d in sorted_geos[:15]:
        roas = d['value'] / d['spend'] if d['spend'] > 0 else 0
        name = d['name'][:48] if d['name'] else rn[-20:]
        print(f"{name:<50}{d['spend']:>9.2f} {d['conv']:>6.1f} {d['value']:>9.2f} {roas:>6.2f}x")

# ====== 3. DEVICE PERFORMANCE ======
print('\n\n=== DEVICE PERFORMANCE ===\n')
devices = defaultdict(lambda: {'spend': 0, 'conv': 0, 'value': 0, 'clicks': 0})
for row in ga_service.search(customer_id=customer_id, query=f'''
    SELECT segments.device,
           metrics.cost_micros, metrics.conversions, metrics.conversions_value, metrics.clicks
    FROM campaign
    WHERE campaign.id = 23781515935
      AND segments.date BETWEEN '{DATE_FROM}' AND '{DATE_TO}'
'''):
    dev = row.segments.device.name
    devices[dev]['spend'] += row.metrics.cost_micros / 1_000_000
    devices[dev]['conv'] += row.metrics.conversions
    devices[dev]['value'] += row.metrics.conversions_value
    devices[dev]['clicks'] += row.metrics.clicks

print(f"{'Device':<15}{'Spend':>10}{'Conv':>7}{'Value':>10}{'ROAS':>7}{'CPA':>8}")
print('-' * 60)
for dev, d in sorted(devices.items(), key=lambda x: -x[1]['spend']):
    roas = d['value'] / d['spend'] if d['spend'] > 0 else 0
    cpa = d['spend'] / d['conv'] if d['conv'] > 0 else 0
    print(f"{dev:<15}{d['spend']:>9.2f} {d['conv']:>6.1f} {d['value']:>9.2f} {roas:>6.2f}x {cpa:>7.2f}")

# ====== 4. CPA SUMMARY ======
print('\n\n=== CPA SUMMARY ===\n')
total_spend = sum(d['spend'] for d in devices.values())
total_conv = sum(d['conv'] for d in devices.values())
total_value = sum(d['value'] for d in devices.values())
print(f'Total spend: ${total_spend:.2f}')
print(f'Total conversions: {total_conv:.1f}')
print(f'Total revenue: ${total_value:.2f}')
print(f'Average CPA: ${total_spend/total_conv:.2f}')
print(f'Average order value (AOV): ${total_value/total_conv:.2f}')
print(f'ROAS: {total_value/total_spend:.2f}x')
print(f'Profit margin per conv (revenue - CPA): ${(total_value-total_spend)/total_conv:.2f}')
