"""Analyze historical lead-gen campaigns for Dr. Ruth Roberts."""
import os
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

from google.ads.googleads.client import GoogleAdsClient

config = {
    "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
    "client_id": os.getenv("GOOGLE_ADS_CLIENT_ID"),
    "client_secret": os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
    "refresh_token": os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
    "login_customer_id": os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "8693027103"),
    "use_proto_plus": True,
}
customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID", "6868436418").replace("-", "")

client = GoogleAdsClient.load_from_dict(config)
ga_service = client.get_service("GoogleAdsService")

# Pull daily-level data for all lead/coaching/optin campaigns with spend
query = """
SELECT
    campaign.id,
    campaign.name,
    campaign.status,
    campaign.advertising_channel_type,
    campaign.bidding_strategy_type,
    metrics.cost_micros,
    metrics.conversions,
    metrics.clicks,
    metrics.impressions,
    segments.date
FROM campaign
WHERE segments.date BETWEEN '2024-04-01' AND '2026-04-20'
  AND metrics.cost_micros > 0
  AND (campaign.name LIKE '%Lead%'
       OR campaign.name LIKE '%lead%'
       OR campaign.name LIKE '%LEAD%'
       OR campaign.name LIKE '%Coaching%'
       OR campaign.name LIKE '%coaching%'
       OR campaign.name LIKE '%Optin%'
       OR campaign.name LIKE '%OPTIN%'
       OR campaign.name LIKE '%optin%'
       OR campaign.name LIKE '%HPHC%'
       OR campaign.name LIKE '%DSA%'
       OR campaign.name LIKE '%Dr ruth%'
       OR campaign.name LIKE '%Dr Ruth%')
"""

agg = defaultdict(lambda: {
    "name": None, "status": None, "channel": None, "bid_strategy": None,
    "cost": 0.0, "conversions": 0.0, "clicks": 0, "impressions": 0,
    "first_date": None, "last_date": None,
})

stream = ga_service.search_stream(customer_id=customer_id, query=query)
for batch in stream:
    for row in batch.results:
        cid = row.campaign.id
        a = agg[cid]
        a["name"] = row.campaign.name
        a["status"] = row.campaign.status.name
        a["channel"] = row.campaign.advertising_channel_type.name
        a["bid_strategy"] = row.campaign.bidding_strategy_type.name
        a["cost"] += row.metrics.cost_micros / 1_000_000
        a["conversions"] += row.metrics.conversions
        a["clicks"] += row.metrics.clicks
        a["impressions"] += row.metrics.impressions
        d = row.segments.date
        if a["first_date"] is None or d < a["first_date"]:
            a["first_date"] = d
        if a["last_date"] is None or d > a["last_date"]:
            a["last_date"] = d

rows = sorted(agg.values(), key=lambda x: -x["cost"])

print(f"\n{'='*120}")
print(f"{'Campaign':<50} {'Status':<10} {'Channel':<14} {'Bid':<22} {'Spend':>10} {'Conv':>7} {'CPA':>9} {'Clicks':>8} {'CTR':>6}  Dates")
print(f"{'='*120}")

total_spend = 0
total_conv = 0
for r in rows:
    cpa = r["cost"] / r["conversions"] if r["conversions"] > 0 else 0
    ctr = r["clicks"] / r["impressions"] if r["impressions"] > 0 else 0
    total_spend += r["cost"]
    total_conv += r["conversions"]
    print(f"{r['name'][:49]:<50} {r['status']:<10} {r['channel'][:13]:<14} {r['bid_strategy'][:21]:<22} "
          f"${r['cost']:>9,.0f} {r['conversions']:>7.1f} ${cpa:>8,.0f} {r['clicks']:>8} {ctr*100:>5.2f}% "
          f" {r['first_date']} to {r['last_date']}")

print(f"\nTOTAL: spend=${total_spend:,.2f}  conversions={total_conv:.1f}  "
      f"avg CPA=${(total_spend/total_conv if total_conv>0 else 0):,.2f}")
print(f"Campaigns found: {len(rows)}")
