"""
Klaviyo Data Collector
======================
Collects email marketing metrics: revenue attribution, open/click rates,
flow performance, list health, and spam complaint rates.

Uses Klaviyo API v2024-10-15 (revision header).
"""

import json
import logging
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

from config import KLAVIYO_API_KEY

logger = logging.getLogger(__name__)

KLAVIYO_BASE = 'https://a.klaviyo.com/api'
KLAVIYO_REVISION = '2024-10-15'


def _klaviyo_get(endpoint, params=None, retries=3):
    """Make an authenticated GET request to the Klaviyo API with rate-limit retry."""
    url = f"{KLAVIYO_BASE}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)

    for attempt in range(retries):
        req = urllib.request.Request(url, headers={
            'Authorization': f'Klaviyo-API-Key {KLAVIYO_API_KEY}',
            'Accept': 'application/json',
            'revision': KLAVIYO_REVISION,
        })
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                wait = int(e.headers.get('Retry-After', 2))
                logger.warning(f"Klaviyo rate limited on {endpoint}, waiting {wait}s...")
                time.sleep(wait)
                continue
            body = e.read().decode('utf-8')[:300]
            logger.error(f"Klaviyo API error {e.code} on {endpoint}: {body}")
            return None


def _klaviyo_post(endpoint, payload):
    """Make an authenticated POST request to the Klaviyo API."""
    url = f"{KLAVIYO_BASE}/{endpoint}"
    data = json.dumps(payload).encode('utf-8')

    req = urllib.request.Request(url, data=data, method='POST', headers={
        'Authorization': f'Klaviyo-API-Key {KLAVIYO_API_KEY}',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'revision': KLAVIYO_REVISION,
    })

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')[:500]
        logger.error(f"Klaviyo POST error {e.code} on {endpoint}: {body}")
        return None


def _get_metric_id_by_name(name):
    """Find a metric ID by its name."""
    data = _klaviyo_get('metrics')
    if not data:
        return None

    for metric in data.get('data', []):
        if metric.get('attributes', {}).get('name', '').lower() == name.lower():
            return metric['id']

    # Paginate if needed
    next_url = data.get('links', {}).get('next')
    while next_url:
        req = urllib.request.Request(next_url, headers={
            'Authorization': f'Klaviyo-API-Key {KLAVIYO_API_KEY}',
            'Accept': 'application/json',
            'revision': KLAVIYO_REVISION,
        })
        try:
            with urllib.request.urlopen(req) as resp:
                page = json.loads(resp.read().decode('utf-8'))
                for metric in page.get('data', []):
                    if metric.get('attributes', {}).get('name', '').lower() == name.lower():
                        return metric['id']
                next_url = page.get('links', {}).get('next')
        except urllib.error.HTTPError:
            break

    return None


def _query_metric_aggregate(metric_id, start_iso, end_iso, measurement='sum_value', extra_filters=None):
    """Query aggregate metric data for a date range with optional dimension filters."""
    filters = [
        f"greater-or-equal(datetime,{start_iso})",
        f"less-than(datetime,{end_iso})",
    ]
    if extra_filters:
        filters.extend(extra_filters)

    payload = {
        "data": {
            "type": "metric-aggregate",
            "attributes": {
                "metric_id": metric_id,
                "measurements": [measurement],
                "interval": "day",
                "filter": filters,
            }
        }
    }
    result = _klaviyo_post('metric-aggregates', payload)
    if not result:
        return 0.0

    data = result.get('data', {}).get('attributes', {}).get('data', [])
    total = 0.0
    for series in data:
        measurements = series.get('measurements', {})
        values = measurements.get(measurement, [])
        total += sum(v for v in values if v is not None)

    return total


def _get_campaign_stats(start_iso, end_iso):
    """Get campaign-level email stats for the date range using the reporting API."""
    payload = {
        "data": {
            "type": "campaign-values-report",
            "attributes": {
                "timeframe": {
                    "start": start_iso,
                    "end": end_iso,
                },
                "conversion_metric_id": _get_metric_id_by_name("Placed Order") or "",
                "statistics": [
                    "opens", "opens_unique", "clicks", "clicks_unique",
                    "recipients", "delivered", "bounced", "spam_complaints",
                    "unsubscribes",
                ],
            }
        }
    }
    result = _klaviyo_post('campaign-values-reports', payload)
    if not result:
        return {}

    # Aggregate across all campaigns
    totals = {
        'opens': 0, 'opens_unique': 0, 'clicks': 0, 'clicks_unique': 0,
        'recipients': 0, 'delivered': 0, 'bounced': 0,
        'spam_complaints': 0, 'unsubscribes': 0,
    }

    for row in result.get('data', {}).get('attributes', {}).get('results', []):
        stats = row.get('statistics', {})
        for key in totals:
            totals[key] += stats.get(key, 0)

    return totals


def _get_list_stats():
    """Get total list size across all lists."""
    data = _klaviyo_get('lists')
    if not data:
        return 0

    total_profiles = 0
    for lst in data.get('data', []):
        list_id = lst['id']
        # Get profile count for each list
        count_data = _klaviyo_get(f'lists/{list_id}', {
            'additional-fields[list]': 'profile_count',
        })
        if count_data:
            attrs = count_data.get('data', {}).get('attributes', {})
            total_profiles += attrs.get('profile_count', 0)
        time.sleep(0.5)  # Avoid rate limits between list requests

    return total_profiles


def _get_flow_revenue(flow_name_contains, metric_id, start_iso, end_iso):
    """Get revenue attributed to flows matching a name pattern."""
    if not metric_id:
        return 0.0

    payload = {
        "data": {
            "type": "metric-aggregate",
            "attributes": {
                "metric_id": metric_id,
                "measurements": ["sum_value"],
                "interval": "day",
                "by": ["$flow"],
                "filter": [
                    f"greater-or-equal(datetime,{start_iso})",
                    f"less-than(datetime,{end_iso})",
                ],
            }
        }
    }
    result = _klaviyo_post('metric-aggregates', payload)
    if not result:
        return 0.0

    total = 0.0
    for series in result.get('data', {}).get('attributes', {}).get('data', []):
        dims = series.get('dimensions', [])
        flow_id = dims[0] if dims else ''
        # We can't easily filter by name in the aggregate query,
        # so we sum all flow-attributed revenue
        measurements = series.get('measurements', {})
        values = measurements.get('sum_value', [])
        total += sum(v for v in values if v is not None)

    return total


def collect_weekly_data(week_ending_date=None):
    """
    Collect all Klaviyo metrics for a given week.

    Returns:
        dict with all Klaviyo weekly metrics
    """
    if week_ending_date is None:
        today = datetime.utcnow().date()
        days_since_sunday = (today.weekday() + 1) % 7
        week_ending_date = today - timedelta(days=days_since_sunday)

    week_start = week_ending_date - timedelta(days=6)
    start_iso = f"{week_start}T00:00:00+00:00"
    end_iso = f"{week_ending_date}T23:59:59+00:00"

    logger.info(f"Collecting Klaviyo data for week {week_start} to {week_ending_date}")

    # ── Find key metric IDs ──────────────────────────────────────────────
    placed_order_id = _get_metric_id_by_name("Placed Order")
    logger.info(f"Placed Order metric ID: {placed_order_id}")

    # ── Email-attributed revenue ─────────────────────────────────────────
    email_revenue = 0.0
    total_placed_order_revenue = 0.0
    if placed_order_id:
        # Total placed order revenue (all channels)
        total_placed_order_revenue = _query_metric_aggregate(
            placed_order_id, start_iso, end_iso, 'sum_value'
        )
        # Email-attributed revenue only (filtered by attribution channel)
        email_revenue = _query_metric_aggregate(
            placed_order_id, start_iso, end_iso, 'sum_value',
            extra_filters=['equals($attributed_channel,"email")']
        )
        logger.info(f"Klaviyo total placed order rev: ${total_placed_order_revenue:.2f}")
        logger.info(f"Klaviyo email-attributed rev: ${email_revenue:.2f}")

    # ── Flow revenue breakdown ───────────────────────────────────────────
    welcome_flow_revenue = 0.0
    abandon_cart_flow_revenue = 0.0
    post_purchase_flow_revenue = 0.0
    if placed_order_id:
        # Get all flow-attributed revenue
        # Note: individual flow breakdown requires flow IDs or the flow report API
        total_flow_revenue = _get_flow_revenue(
            'all', placed_order_id, start_iso, end_iso
        )
        # For now, set total as email revenue; granular breakdown needs flow IDs
        logger.info(f"Total flow-attributed revenue: ${total_flow_revenue:.2f}")

    # ── Campaign email stats ─────────────────────────────────────────────
    stats = _get_campaign_stats(start_iso, end_iso)

    delivered = stats.get('delivered', 0)
    opens_unique = stats.get('opens_unique', 0)
    clicks_unique = stats.get('clicks_unique', 0)
    recipients = stats.get('recipients', 0)
    bounced = stats.get('bounced', 0)
    spam = stats.get('spam_complaints', 0)

    open_rate = (opens_unique / delivered) if delivered > 0 else 0.0
    click_rate = (clicks_unique / delivered) if delivered > 0 else 0.0
    ctor = (clicks_unique / opens_unique) if opens_unique > 0 else 0.0
    delivery_rate = (delivered / recipients) if recipients > 0 else 0.0
    spam_complaint_rate = (spam / delivered) if delivered > 0 else 0.0

    # ── List health ──────────────────────────────────────────────────────
    list_size = _get_list_stats()

    # List growth rate requires previous week's data (calculated in cross-platform)
    list_growth_rate = 0.0

    # ── Abandon cart recovery ────────────────────────────────────────────
    # This would require flow-specific metrics; placeholder for now
    abandon_cart_recovery_rate = 0.0
    abandon_cart_recovery_revenue = abandon_cart_flow_revenue

    # ── Assemble results ─────────────────────────────────────────────────
    result = {
        'week_ending_date': str(week_ending_date),
        'email_attributed_revenue': round(email_revenue, 2),
        'total_placed_order_revenue': round(total_placed_order_revenue, 2),
        'welcome_flow_revenue': round(welcome_flow_revenue, 2),
        'abandon_cart_flow_revenue': round(abandon_cart_flow_revenue, 2),
        'post_purchase_flow_revenue': round(post_purchase_flow_revenue, 2),
        'open_rate': round(open_rate, 4),
        'click_rate': round(click_rate, 4),
        'ctor': round(ctor, 4),
        'list_size': list_size,
        'list_growth_rate': round(list_growth_rate, 4),
        'spam_complaint_rate': round(spam_complaint_rate, 6),
        'delivery_rate': round(delivery_rate, 4),
        'abandon_cart_recovery_rate': round(abandon_cart_recovery_rate, 4),
        'abandon_cart_recovery_revenue': round(abandon_cart_recovery_revenue, 2),
    }

    logger.info(f"Klaviyo collection complete: ${email_revenue:.2f} email revenue, "
                f"list size {list_size}, open rate {open_rate:.1%}")

    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2))
