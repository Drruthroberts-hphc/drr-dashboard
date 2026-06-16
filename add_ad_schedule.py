#!/usr/bin/env python3
"""Add ad schedule to block hours 6 AM and 9 PM (21:00) across all days.
   Creates allow-ranges: 00-06, 07-21, 22-24 for each day of week.
"""
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
campaign_rn = 'customers/6868436418/campaigns/23781515935'

cc_service = client.get_service('CampaignCriterionService')
day_enum = client.enums.DayOfWeekEnum
minute_enum = client.enums.MinuteOfHourEnum

# Days of week (Google enum)
DAYS = [
    day_enum.MONDAY,
    day_enum.TUESDAY,
    day_enum.WEDNESDAY,
    day_enum.THURSDAY,
    day_enum.FRIDAY,
    day_enum.SATURDAY,
    day_enum.SUNDAY,
]

# Allow ranges (block 6 AM = hour 6, block 9 PM = hour 21)
RANGES = [
    (0, 6),   # 00:00 - 06:00 (blocks hour 6)
    (7, 21),  # 07:00 - 21:00 (blocks hour 21)
    (22, 24), # 22:00 - 24:00
]

ops = []
for day in DAYS:
    for start_h, end_h in RANGES:
        op = client.get_type('CampaignCriterionOperation')
        op.create.campaign = campaign_rn
        op.create.ad_schedule.day_of_week = day
        op.create.ad_schedule.start_hour = start_h
        op.create.ad_schedule.start_minute = minute_enum.ZERO
        op.create.ad_schedule.end_hour = end_h
        op.create.ad_schedule.end_minute = minute_enum.ZERO
        ops.append(op)

print(f'Creating {len(ops)} ad schedule criteria (3 ranges × 7 days)...')
try:
    response = cc_service.mutate_campaign_criteria(customer_id=customer_id, operations=ops)
    print(f'✅ SUCCESS — {len(response.results)} schedule criteria created')
except Exception as e:
    print(f'❌ Failed: {e}')
    raise

# Verify
print('\n=== VERIFICATION ===')
ga_service = client.get_service('GoogleAdsService')
schedules = []
for row in ga_service.search(customer_id=customer_id, query='''
    SELECT campaign_criterion.criterion_id,
           campaign_criterion.ad_schedule.day_of_week,
           campaign_criterion.ad_schedule.start_hour,
           campaign_criterion.ad_schedule.end_hour,
           campaign_criterion.status
    FROM campaign_criterion
    WHERE campaign.id = 23781515935
      AND campaign_criterion.type = 'AD_SCHEDULE'
    ORDER BY campaign_criterion.ad_schedule.day_of_week,
             campaign_criterion.ad_schedule.start_hour
'''):
    s = row.campaign_criterion.ad_schedule
    schedules.append((s.day_of_week.name, s.start_hour, s.end_hour, row.campaign_criterion.status.name))

print(f'Total active schedules: {len(schedules)}')
for day, sh, eh, status in schedules:
    print(f'  {day}: {sh:02d}:00 - {eh:02d}:00 [{status}]')
