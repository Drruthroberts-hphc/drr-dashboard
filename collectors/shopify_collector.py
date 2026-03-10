"""
Shopify Data Collector
=======================
Collects orders, revenue by silo, customers, refunds, and product data
from the Shopify Admin API.
"""

import json
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict

from config import (
    SHOPIFY_BASE_URL, SHOPIFY_ACCESS_TOKEN, classify_product_silo
)

logger = logging.getLogger(__name__)


def _shopify_get(endpoint, params=None):
    """Make an authenticated GET request to the Shopify Admin API."""
    url = f"{SHOPIFY_BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        'Content-Type': 'application/json',
    })

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')[:300]
        logger.error(f"Shopify API error {e.code} on {endpoint}: {body}")
        return None


def _shopify_get_all(endpoint, resource_key, params=None):
    """Paginate through all results using Shopify's link-based pagination."""
    params = params or {}
    params.setdefault('limit', 250)
    all_items = []

    url = f"{SHOPIFY_BASE_URL}/{endpoint}?" + urllib.parse.urlencode(params)

    while url:
        req = urllib.request.Request(url, headers={
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
            'Content-Type': 'application/json',
        })

        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                items = data.get(resource_key, [])
                all_items.extend(items)

                # Check for next page via Link header
                link_header = resp.headers.get('Link', '')
                url = None
                if 'rel="next"' in link_header:
                    for part in link_header.split(','):
                        if 'rel="next"' in part:
                            url = part.split('<')[1].split('>')[0]
                            break
        except urllib.error.HTTPError as e:
            logger.error(f"Shopify pagination error {e.code}: {e.read().decode()[:200]}")
            break

    return all_items


def _build_product_silo_map():
    """
    Build a mapping of product_id -> silo from all Shopify products.
    Uses the classification rules in config.py.
    """
    logger.info("Building product silo map from Shopify products...")
    products = _shopify_get_all('products.json', 'products', {
        'fields': 'id,title,vendor,product_type,status',
        'limit': 250,
    })

    silo_map = {}
    counts = defaultdict(int)

    for p in products:
        silo = classify_product_silo(
            p.get('title', ''),
            p.get('vendor', ''),
            p.get('product_type', ''),
        )
        silo_map[p['id']] = silo
        counts[silo] += 1

    logger.info(f"Product silo map: {dict(counts)} ({len(silo_map)} total products)")
    return silo_map


def collect_weekly_data(week_ending_date=None):
    """
    Collect all Shopify metrics for a given week.

    Args:
        week_ending_date: datetime for the end of the week (Sunday).
                         Defaults to most recent Sunday.

    Returns:
        dict with all Shopify weekly metrics
    """
    if week_ending_date is None:
        today = datetime.utcnow().date()
        # Most recent Sunday
        days_since_sunday = (today.weekday() + 1) % 7
        week_ending_date = today - timedelta(days=days_since_sunday)

    week_start = week_ending_date - timedelta(days=6)
    start_iso = f"{week_start}T00:00:00Z"
    end_iso = f"{week_ending_date}T23:59:59Z"

    logger.info(f"Collecting Shopify data for week {week_start} to {week_ending_date}")

    # Build product -> silo mapping
    silo_map = _build_product_silo_map()

    # ── Orders ─────────────────────────────────────────────────────────
    orders = _shopify_get_all('orders.json', 'orders', {
        'status': 'any',
        'financial_status': 'paid',
        'created_at_min': start_iso,
        'created_at_max': end_iso,
        'fields': 'id,total_price,subtotal_price,total_discounts,total_tax,'
                  'line_items,refunds,customer,created_at',
    })

    # Revenue calculations
    gross_revenue = 0.0
    net_revenue = 0.0
    total_discounts = 0.0
    silo_revenue = defaultdict(float)
    product_revenue = defaultdict(float)
    order_count = len(orders)

    for order in orders:
        order_gross = float(order.get('total_price', 0))
        order_net = float(order.get('subtotal_price', 0))
        order_disc = float(order.get('total_discounts', 0))

        gross_revenue += order_gross
        net_revenue += order_net
        total_discounts += order_disc

        # Classify each line item into a silo
        for item in order.get('line_items', []):
            product_id = item.get('product_id')
            line_total = float(item.get('price', 0)) * int(item.get('quantity', 1))
            silo = silo_map.get(product_id, 'E-Commerce')
            silo_revenue[silo] += line_total

            # Track product revenue for top 10
            product_name = item.get('title', 'Unknown')
            product_revenue[product_name] += line_total

    # AOV
    aov = gross_revenue / order_count if order_count > 0 else 0.0

    # Top 10 products by revenue
    top_products = sorted(product_revenue.items(), key=lambda x: x[1], reverse=True)[:10]
    top_products_json = json.dumps([
        {'name': name, 'revenue': round(rev, 2)} for name, rev in top_products
    ])

    # Discount rate
    discount_rate = (total_discounts / gross_revenue * 100) if gross_revenue > 0 else 0.0

    # ── Refunds ────────────────────────────────────────────────────────
    refund_count = 0
    for order in orders:
        if order.get('refunds'):
            refund_count += 1
    return_rate = (refund_count / order_count * 100) if order_count > 0 else 0.0

    # ── Customers ──────────────────────────────────────────────────────
    customers = _shopify_get_all('customers.json', 'customers', {
        'created_at_min': start_iso,
        'created_at_max': end_iso,
        'fields': 'id,orders_count',
    })

    new_customers = sum(1 for c in customers if int(c.get('orders_count', 0)) <= 1)
    returning_customers = sum(1 for c in customers if int(c.get('orders_count', 0)) > 1)

    # ── Checkouts (abandonment) ────────────────────────────────────────
    # Note: Checkout API may have limited access; handle gracefully
    cart_abandonment_rate = 0.0
    checkout_abandonment_rate = 0.0
    try:
        checkouts = _shopify_get_all('checkouts.json', 'checkouts', {
            'created_at_min': start_iso,
            'created_at_max': end_iso,
        })
        if checkouts:
            abandoned = sum(1 for c in checkouts if not c.get('completed_at'))
            checkout_abandonment_rate = (abandoned / len(checkouts) * 100) if checkouts else 0.0
    except Exception as e:
        logger.warning(f"Could not fetch checkout data: {e}")

    # ── Conversion Rate ────────────────────────────────────────────────
    # Requires read_analytics scope — may not be available via REST API
    conversion_rate = 0.0

    # ── Assemble results ───────────────────────────────────────────────
    result = {
        'week_ending_date': str(week_ending_date),
        'gross_revenue': round(gross_revenue, 2),
        'net_revenue': round(net_revenue, 2),
        'ecommerce_revenue': round(silo_revenue.get('E-Commerce', 0), 2),
        'coaching_revenue': round(silo_revenue.get('Coaching', 0), 2),
        'course_revenue': round(silo_revenue.get('Courses', 0), 2),
        'order_count': order_count,
        'aov': round(aov, 2),
        'conversion_rate': round(conversion_rate, 4),
        'new_customers': new_customers,
        'returning_customers': returning_customers,
        'cart_abandonment_rate': round(cart_abandonment_rate, 2),
        'checkout_abandonment_rate': round(checkout_abandonment_rate, 2),
        'return_rate': round(return_rate, 2),
        'discount_rate_pct': round(discount_rate, 2),
        'top_products_json': top_products_json,
    }

    logger.info(f"Shopify collection complete: ${gross_revenue:.2f} gross, "
                f"{order_count} orders, AOV ${aov:.2f}")

    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2))
