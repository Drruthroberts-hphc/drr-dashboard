"""
Google Ads Collector
====================
Collects weekly Google Ads performance data via the Google Ads API.

Metrics collected:
- Ad spend, conversions, conversion value, ROAS, CPA
- Clicks, impressions, CTR
- Shopping product status (active vs disapproved)

Requires google-ads Python library and credentials in .env:
  GOOGLE_ADS_DEVELOPER_TOKEN
  GOOGLE_ADS_CLIENT_ID
  GOOGLE_ADS_CLIENT_SECRET
  GOOGLE_ADS_REFRESH_TOKEN
  GOOGLE_ADS_CUSTOMER_ID
  GOOGLE_ADS_LOGIN_CUSTOMER_ID (optional, for MCC accounts)
"""

import logging
from datetime import timedelta

logger = logging.getLogger(__name__)


def _get_client():
    """Create and return a Google Ads API client."""
    try:
        from google.ads.googleads.client import GoogleAdsClient
    except ImportError:
        logger.error(
            "google-ads library not installed. "
            "Run: pip install google-ads"
        )
        return None

    from config import (
        GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CLIENT_ID,
        GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN,
        GOOGLE_ADS_CUSTOMER_ID, GOOGLE_ADS_LOGIN_CUSTOMER_ID,
    )

    if not GOOGLE_ADS_DEVELOPER_TOKEN or not GOOGLE_ADS_CUSTOMER_ID:
        logger.warning("Google Ads credentials not configured — skipping")
        return None

    config = {
        "developer_token": GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id": GOOGLE_ADS_CLIENT_ID,
        "client_secret": GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token": GOOGLE_ADS_REFRESH_TOKEN,
        "use_proto_plus": True,
    }
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        config["login_customer_id"] = GOOGLE_ADS_LOGIN_CUSTOMER_ID

    try:
        client = GoogleAdsClient.load_from_dict(config)
        return client
    except Exception as e:
        logger.error(f"Failed to create Google Ads client: {e}")
        return None


def _query_campaign_performance(client, customer_id, start_date, end_date):
    """Query campaign-level performance metrics for the date range."""
    ga_service = client.get_service("GoogleAdsService")

    query = f"""
        SELECT
            metrics.cost_micros,
            metrics.conversions,
            metrics.conversions_value,
            metrics.clicks,
            metrics.impressions,
            metrics.ctr
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND campaign.status = 'ENABLED'
    """

    total_cost_micros = 0
    total_conversions = 0.0
    total_conversion_value = 0.0
    total_clicks = 0
    total_impressions = 0

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            total_cost_micros += row.metrics.cost_micros
            total_conversions += row.metrics.conversions
            total_conversion_value += row.metrics.conversions_value
            total_clicks += row.metrics.clicks
            total_impressions += row.metrics.impressions
    except Exception as e:
        logger.error(f"Google Ads campaign query failed: {e}")
        return {}

    total_spend = total_cost_micros / 1_000_000
    roas = (total_conversion_value / total_spend) if total_spend > 0 else 0
    cpa = (total_spend / total_conversions) if total_conversions > 0 else 0
    ctr = (total_clicks / total_impressions) if total_impressions > 0 else 0

    return {
        'ad_spend': round(total_spend, 2),
        'conversions': round(total_conversions, 1),
        'conversion_value': round(total_conversion_value, 2),
        'roas': round(roas, 2),
        'cpa': round(cpa, 2),
        'clicks': total_clicks,
        'impressions': total_impressions,
        'ctr': round(ctr, 4),
    }


def _query_shopping_product_status(client, customer_id):
    """Query Shopping product status to find disapproval rate."""
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
            shopping_product.resource_name,
            shopping_product.status
        FROM shopping_product
    """

    active = 0
    disapproved = 0
    total = 0

    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            total += 1
            status = row.shopping_product.status
            # Status enum: ELIGIBLE, NOT_ELIGIBLE, ACTIVE, etc.
            if status in (2, 'ELIGIBLE', 'ACTIVE'):  # varies by API version
                active += 1
            else:
                disapproved += 1
    except Exception as e:
        # Shopping product query may fail if no Shopping campaigns exist
        logger.warning(f"Shopping product status query failed: {e}")
        # Use known values as fallback
        return {
            'active_products': 0,
            'disapproved_products': 0,
            'disapproval_rate': 0.0,
        }

    disapproval_rate = (disapproved / total) if total > 0 else 0

    return {
        'active_products': active,
        'disapproved_products': disapproved,
        'disapproval_rate': round(disapproval_rate, 4),
    }


def collect_weekly_data(week_ending_date):
    """
    Collect Google Ads data for the week ending on the given date.

    Args:
        week_ending_date: date object (Sunday)

    Returns:
        dict with Google Ads metrics
    """
    from config import GOOGLE_ADS_CUSTOMER_ID

    client = _get_client()
    if client is None:
        logger.warning("Google Ads collector: no client available, returning empty data")
        return _empty_data(week_ending_date)

    customer_id = GOOGLE_ADS_CUSTOMER_ID.replace('-', '')

    # Week = Monday to Sunday
    start_date = (week_ending_date - timedelta(days=6)).strftime('%Y-%m-%d')
    end_date = week_ending_date.strftime('%Y-%m-%d')

    logger.info(f"Google Ads: collecting {start_date} to {end_date}")

    # Campaign performance
    perf = _query_campaign_performance(client, customer_id, start_date, end_date)

    # Shopping product status (point-in-time, not date-range)
    products = _query_shopping_product_status(client, customer_id)

    result = {
        'week_ending_date': str(week_ending_date),
        **perf,
        **products,
    }

    # Add the ROAS metric under a name the alert system can find
    result['google_ads_roas'] = result.get('roas', 0)
    result['google_ads_weekly_spend'] = result.get('ad_spend', 0)

    logger.info(
        f"Google Ads: spend=${result.get('ad_spend', 0):,.2f}, "
        f"ROAS={result.get('roas', 0):.1f}x, "
        f"disapproval={result.get('disapproval_rate', 0):.0%}"
    )

    return result


def _empty_data(week_ending_date):
    """Return empty data structure when API is unavailable."""
    return {
        'week_ending_date': str(week_ending_date),
        'ad_spend': 0,
        'conversions': 0,
        'conversion_value': 0,
        'roas': 0,
        'cpa': 0,
        'clicks': 0,
        'impressions': 0,
        'ctr': 0,
        'active_products': 0,
        'disapproved_products': 0,
        'disapproval_rate': 0,
        'google_ads_roas': 0,
        'google_ads_weekly_spend': 0,
    }
