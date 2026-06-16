"""Weekly Google Ads report — this week vs last week."""
import sys
import os
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collectors.google_ads_collector import _get_client, _query_shopping_product_status
from config import GOOGLE_ADS_CUSTOMER_ID


def query_per_campaign(client, customer_id, start, end):
    ga = client.get_service("GoogleAdsService")
    q = f"""
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value,
          metrics.clicks,
          metrics.impressions
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'
    """
    rows = {}
    try:
        for r in ga.search(customer_id=customer_id, query=q):
            cid = r.campaign.id
            if cid not in rows:
                rows[cid] = {
                    'name': r.campaign.name,
                    'status': str(r.campaign.status),
                    'channel': str(r.campaign.advertising_channel_type),
                    'spend': 0.0, 'conv': 0.0, 'val': 0.0, 'clicks': 0, 'impr': 0,
                }
            rows[cid]['spend'] += r.metrics.cost_micros / 1_000_000
            rows[cid]['conv'] += r.metrics.conversions
            rows[cid]['val'] += r.metrics.conversions_value
            rows[cid]['clicks'] += r.metrics.clicks
            rows[cid]['impr'] += r.metrics.impressions
    except Exception as e:
        print(f"ERR per-campaign: {e}")
        return {}
    return rows


def query_daily_totals(client, customer_id, start, end):
    ga = client.get_service("GoogleAdsService")
    q = f"""
        SELECT
          segments.date,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'
          AND campaign.status = 'ENABLED'
    """
    days = {}
    try:
        for r in ga.search(customer_id=customer_id, query=q):
            d = r.segments.date
            if d not in days:
                days[d] = {'spend': 0.0, 'conv': 0.0, 'val': 0.0}
            days[d]['spend'] += r.metrics.cost_micros / 1_000_000
            days[d]['conv'] += r.metrics.conversions
            days[d]['val'] += r.metrics.conversions_value
    except Exception as e:
        print(f"ERR daily: {e}")
    return days


def query_asset_group_perf(client, customer_id, start, end):
    ga = client.get_service("GoogleAdsService")
    q = f"""
        SELECT
          asset_group.name,
          campaign.name,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value,
          metrics.clicks,
          metrics.impressions
        FROM asset_group
        WHERE segments.date BETWEEN '{start}' AND '{end}'
    """
    groups = {}
    try:
        for r in ga.search(customer_id=customer_id, query=q):
            key = f"{r.campaign.name} :: {r.asset_group.name}"
            if key not in groups:
                groups[key] = {'spend': 0.0, 'conv': 0.0, 'val': 0.0, 'clicks': 0, 'impr': 0}
            groups[key]['spend'] += r.metrics.cost_micros / 1_000_000
            groups[key]['conv'] += r.metrics.conversions
            groups[key]['val'] += r.metrics.conversions_value
            groups[key]['clicks'] += r.metrics.clicks
            groups[key]['impr'] += r.metrics.impressions
    except Exception as e:
        print(f"ERR asset_group: {e}")
    return groups


def totals(rows):
    t = {'spend': 0.0, 'conv': 0.0, 'val': 0.0, 'clicks': 0, 'impr': 0}
    for r in rows.values():
        t['spend'] += r['spend']
        t['conv'] += r['conv']
        t['val'] += r['val']
        t['clicks'] += r['clicks']
        t['impr'] += r['impr']
    t['roas'] = (t['val'] / t['spend']) if t['spend'] > 0 else 0
    t['cpa'] = (t['spend'] / t['conv']) if t['conv'] > 0 else 0
    t['ctr'] = (t['clicks'] / t['impr']) if t['impr'] > 0 else 0
    return t


def pct(a, b):
    if b == 0:
        return 'n/a' if a == 0 else '+inf%'
    return f"{((a-b)/b)*100:+.1f}%"


def main():
    today = date(2026, 4, 30)
    this_end = today - timedelta(days=1)        # 2026-04-29
    this_start = this_end - timedelta(days=6)    # 2026-04-23
    prev_end = this_start - timedelta(days=1)    # 2026-04-22
    prev_start = prev_end - timedelta(days=6)    # 2026-04-16

    print(f"Today: {today}")
    print(f"This week:  {this_start} to {this_end}")
    print(f"Prev week:  {prev_start} to {prev_end}")
    print()

    client = _get_client()
    if client is None:
        print("FATAL: Google Ads client unavailable. Check credentials / refresh token.")
        return 1

    cid = GOOGLE_ADS_CUSTOMER_ID.replace('-', '')

    # Per-campaign for both weeks
    print("=== Per-campaign THIS WEEK ===")
    this_rows = query_per_campaign(client, cid, this_start.strftime('%Y-%m-%d'), this_end.strftime('%Y-%m-%d'))
    for cid_k, r in sorted(this_rows.items(), key=lambda x: -x[1]['spend']):
        roas = (r['val']/r['spend']) if r['spend'] > 0 else 0
        print(f"  [{r['status']}] {r['name'][:60]:60} ch={r['channel'][:12]:12} spend=${r['spend']:.2f} conv={r['conv']:.1f} val=${r['val']:.2f} ROAS={roas:.2f}x")

    print("\n=== Per-campaign PREV WEEK ===")
    prev_rows = query_per_campaign(client, cid, prev_start.strftime('%Y-%m-%d'), prev_end.strftime('%Y-%m-%d'))
    for cid_k, r in sorted(prev_rows.items(), key=lambda x: -x[1]['spend']):
        roas = (r['val']/r['spend']) if r['spend'] > 0 else 0
        print(f"  [{r['status']}] {r['name'][:60]:60} ch={r['channel'][:12]:12} spend=${r['spend']:.2f} conv={r['conv']:.1f} val=${r['val']:.2f} ROAS={roas:.2f}x")

    # Filter to ENABLED only for headline
    this_enabled = {k: v for k, v in this_rows.items() if 'ENABLED' in v['status']}
    prev_enabled = {k: v for k, v in prev_rows.items() if 'ENABLED' in v['status']}

    t = totals(this_enabled)
    p = totals(prev_enabled)
    print("\n=== HEADLINE (enabled campaigns only) ===")
    print(f"  spend:   ${t['spend']:.2f}  vs  ${p['spend']:.2f}   ({pct(t['spend'], p['spend'])})")
    print(f"  conv:    {t['conv']:.1f}    vs  {p['conv']:.1f}     ({pct(t['conv'], p['conv'])})")
    print(f"  val:     ${t['val']:.2f}    vs  ${p['val']:.2f}     ({pct(t['val'], p['val'])})")
    print(f"  ROAS:    {t['roas']:.2f}x   vs  {p['roas']:.2f}x    (Δ {t['roas']-p['roas']:+.2f}x)")
    print(f"  CPA:     ${t['cpa']:.2f}    vs  ${p['cpa']:.2f}")
    print(f"  CTR:     {t['ctr']*100:.2f}%  vs  {p['ctr']*100:.2f}%")
    print(f"  clicks:  {t['clicks']}      vs  {p['clicks']}")
    print(f"  impr:    {t['impr']}        vs  {p['impr']}")

    # Daily for ROAS-day tracking + spend pacing alerts
    print("\n=== DAILY (this week, enabled campaigns) ===")
    daily = query_daily_totals(client, cid, this_start.strftime('%Y-%m-%d'), this_end.strftime('%Y-%m-%d'))
    days_below_2 = 0
    days_over_budget_2x = 0
    for d in sorted(daily.keys()):
        row = daily[d]
        roas = (row['val']/row['spend']) if row['spend'] > 0 else 0
        flag = ''
        if row['spend'] > 0 and roas < 2.0:
            days_below_2 += 1
            flag += ' LOWROAS'
        if row['spend'] > 150:  # 2x of $75 daily budget
            days_over_budget_2x += 1
            flag += ' OVERBUDGET'
        print(f"  {d}  spend=${row['spend']:.2f}  conv={row['conv']:.1f}  val=${row['val']:.2f}  ROAS={roas:.2f}x{flag}")
    print(f"  DAYS_ROAS_BELOW_2: {days_below_2}")
    print(f"  DAYS_SPEND_OVER_2X_BUDGET: {days_over_budget_2x}")

    # Asset groups
    print("\n=== ASSET GROUPS (this week) ===")
    ags = query_asset_group_perf(client, cid, this_start.strftime('%Y-%m-%d'), this_end.strftime('%Y-%m-%d'))
    for k, r in sorted(ags.items(), key=lambda x: -x[1]['spend']):
        roas = (r['val']/r['spend']) if r['spend'] > 0 else 0
        print(f"  {k[:80]:80} spend=${r['spend']:.2f} conv={r['conv']:.1f} val=${r['val']:.2f} ROAS={roas:.2f}x")

    # Product status
    print("\n=== SHOPPING PRODUCT STATUS (point-in-time) ===")
    ps = _query_shopping_product_status(client, cid)
    print(f"  approved={ps.get('active_products')}  disapproved={ps.get('disapproved_products')}  rate={ps.get('disapproval_rate'):.2%}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
