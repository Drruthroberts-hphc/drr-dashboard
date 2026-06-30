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


def _query_metric_aggregate(metric_id, start_iso, end_iso, measurement='sum_value',
                             extra_filters=None, group_by=None, dimension_filter=None):
    """Query aggregate metric data for a date range with optional grouping.

    Args:
        group_by: list of dimension names to group by (e.g. ['$attributed_channel'])
        dimension_filter: tuple of (dimension_value) to sum only that group.
            E.g. '$email_channel' to get only email-attributed values.
    """
    filters = [
        f"greater-or-equal(datetime,{start_iso})",
        f"less-than(datetime,{end_iso})",
    ]
    if extra_filters:
        filters.extend(extra_filters)

    attributes = {
        "metric_id": metric_id,
        "measurements": [measurement],
        "interval": "day",
        "filter": filters,
    }
    if group_by:
        attributes["by"] = group_by

    payload = {
        "data": {
            "type": "metric-aggregate",
            "attributes": attributes,
        }
    }
    result = _klaviyo_post('metric-aggregates', payload)
    if not result:
        return 0.0

    data = result.get('data', {}).get('attributes', {}).get('data', [])
    total = 0.0
    for series in data:
        # If dimension_filter is set, only sum matching groups
        if dimension_filter and group_by:
            dimensions = series.get('dimensions', [])
            if dimension_filter not in dimensions:
                continue
        measurements_data = series.get('measurements', {})
        values = measurements_data.get(measurement, [])
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
    """
    Get the size of the MAIN newsletter list (not sum of all lists — that
    triple-counts people on multiple segments).
    Strategy: find the largest list (typically the master newsletter list)
    and use its count as the headline number.
    """
    data = _klaviyo_get('lists')
    if not data:
        return 0

    list_sizes = []  # [(name, count), ...]
    for lst in data.get('data', []):
        list_id = lst['id']
        list_name = lst.get('attributes', {}).get('name', '')
        count_data = _klaviyo_get(f'lists/{list_id}', {
            'additional-fields[list]': 'profile_count',
        })
        if count_data:
            attrs = count_data.get('data', {}).get('attributes', {})
            count = attrs.get('profile_count', 0) or 0
            list_sizes.append((list_name, count))
        time.sleep(0.3)

    if not list_sizes:
        return 0

    # Sort descending. The largest list is the master newsletter / subscriber list.
    list_sizes.sort(key=lambda x: -x[1])
    largest_name, largest_count = list_sizes[0]
    logger.info(f"Klaviyo list_size: using largest list '{largest_name}' = {largest_count} "
                f"(of {len(list_sizes)} lists, sum-of-all = {sum(c for _, c in list_sizes)})")
    return largest_count


def _get_total_subscribed_profiles():
    """Get the total count of currently-subscribed profiles via the profiles endpoint."""
    try:
        data = _klaviyo_get('profiles', {
            'filter': "equals(subscriptions.email.marketing.consent,'SUBSCRIBED')",
            'page[size]': 1,
            'fields[profile]': 'id',
            'additional-fields[profile]': 'subscriptions',
        })
        if data:
            # Klaviyo paginates — meta.total is not always provided, but
            # the 'links.next' header can be used to bisect for count.
            # Quickest reliable approach: iterate pages with larger page_size.
            count = 0
            cursor = None
            for _ in range(50):  # safety cap: max 50 pages = 5000 profiles surveyed
                params = {
                    'filter': "equals(subscriptions.email.marketing.consent,'SUBSCRIBED')",
                    'page[size]': 100,
                    'fields[profile]': 'id',
                }
                if cursor:
                    params['page[cursor]'] = cursor
                page = _klaviyo_get('profiles', params)
                if not page:
                    break
                count += len(page.get('data', []))
                cursor = page.get('links', {}).get('next', '')
                if cursor:
                    # Extract cursor token from URL if it's a full URL
                    if 'page[cursor]=' in cursor:
                        cursor = cursor.split('page[cursor]=')[-1].split('&')[0]
                    else:
                        cursor = None
                if not cursor:
                    break
                time.sleep(0.3)
            logger.info(f"Klaviyo total subscribed profiles (paginated): {count}")
            return count
    except Exception as e:
        logger.warning(f"Could not count subscribed profiles: {e}")
    return 0


def _get_engagement_segments():
    """
    Get counts for Ruth's full engagement-decay segment family.

    KNOWN segment IDs (verified 2026-06-30, created May 4 2026):
      XCW39M = 'Engaged v2 (0-60 d)'        ← PRIMARY ENGAGED (~15.7K)
      X3tuCM = 'Engaged v2 (61-90 d) (clone)'
      SL9UJw = 'Engaged v2 (91-120 d)'
      Vk2RuB = 'Engaged v2 (121-180 d) (clone)'
      Rbh7Jq = 'Engaged A List (60days)'    ← old segment (kept for back-compat)

    Returns dict with all engagement-bucket counts + computed totals.
    """
    SEGMENTS = {
        'engaged_0_60d': ('XCW39M', 'Engaged v2 (0-60 d)'),
        'engaged_61_90d': ('X3tuCM', 'Engaged v2 (61-90 d) (clone)'),
        'engaged_91_120d': ('SL9UJw', 'Engaged v2 (91-120 d)'),
        'engaged_121_180d': ('Vk2RuB', 'Engaged v2 (121-180 d) (clone)'),
    }

    out = {}
    for key, (seg_id, name) in SEGMENTS.items():
        detail = _klaviyo_get(f'segments/{seg_id}', {
            'additional-fields[segment]': 'profile_count',
        })
        if detail:
            attrs = detail.get('data', {}).get('attributes', {})
            count = attrs.get('profile_count', 0) or 0
            out[key] = int(count)
            logger.info(f"Klaviyo {name}: {count:,} profiles")
        else:
            out[key] = 0
        time.sleep(0.2)

    # Total active (anyone engaged in last 180 days)
    out['engaged_total_180d'] = sum(out.values())
    # Primary headline number = 0-60 day engagement
    out['engaged_primary'] = out.get('engaged_0_60d', 0)
    return out


def _get_engaged_subscribers():
    """Back-compat wrapper — returns primary (0-60d) engaged count."""
    return _get_engagement_segments().get('engaged_primary', 0)


def _get_subscriber_metrics(start_iso, end_iso):
    """
    Pull weekly subscriber dynamics from Klaviyo standard metrics.

    Returns dict with:
      - new_subscribers_count: total Subscribed-to-Email events in the week
      - unsubscribed_count: total Unsubscribed events in the week
      - net_new_subscribers: new - unsubscribed
      - active_subscribers: total profiles currently opted in to email marketing
    """
    out = {
        'new_subscribers_count': 0,
        'unsubscribed_count': 0,
        'net_new_subscribers': 0,
        'active_subscribers': 0,
    }

    # Count new subscribers via the "Subscribed to List" metric
    subscribed_id = _get_metric_id_by_name("Subscribed to List")
    if subscribed_id:
        new_count = _query_metric_aggregate(
            subscribed_id, start_iso, end_iso, measurement='count'
        )
        out['new_subscribers_count'] = int(new_count or 0)
        logger.info(f"Klaviyo new subscribers this week: {out['new_subscribers_count']}")

    # Count unsubscribes
    unsub_id = _get_metric_id_by_name("Unsubscribed from List")
    if unsub_id:
        unsub_count = _query_metric_aggregate(
            unsub_id, start_iso, end_iso, measurement='count'
        )
        out['unsubscribed_count'] = int(unsub_count or 0)

    out['net_new_subscribers'] = out['new_subscribers_count'] - out['unsubscribed_count']

    # Active subscribers — use paginated count
    out['active_subscribers'] = _get_total_subscribed_profiles()

    return out


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
        # Email-attributed revenue only (grouped by channel, filtered to email)
        email_revenue = _query_metric_aggregate(
            placed_order_id, start_iso, end_iso, 'sum_value',
            group_by=['$attributed_channel'],
            dimension_filter='$email_channel'
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

    # ── Subscriber dynamics ──────────────────────────────────────────────
    sub_metrics = _get_subscriber_metrics(start_iso, end_iso)
    engagement_buckets = _get_engagement_segments()
    engaged_subscribers = engagement_buckets.get('engaged_primary', 0)

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
        'new_subscribers_count': sub_metrics['new_subscribers_count'],
        'unsubscribed_count': sub_metrics['unsubscribed_count'],
        'net_new_subscribers': sub_metrics['net_new_subscribers'],
        'active_subscribers': sub_metrics['active_subscribers'],
        'engaged_subscribers': engaged_subscribers,
        'engaged_0_60d': engagement_buckets.get('engaged_0_60d', 0),
        'engaged_61_90d': engagement_buckets.get('engaged_61_90d', 0),
        'engaged_91_120d': engagement_buckets.get('engaged_91_120d', 0),
        'engaged_121_180d': engagement_buckets.get('engaged_121_180d', 0),
        'engaged_total_180d': engagement_buckets.get('engaged_total_180d', 0),
    }

    logger.info(f"Klaviyo collection complete: ${email_revenue:.2f} email revenue, "
                f"list size {list_size}, open rate {open_rate:.1%}")

    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2))
