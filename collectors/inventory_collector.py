"""
Inventory Data Collector
=========================
Pulls product inventory levels, sales velocity, and calculates reorder points
from the Shopify Admin API. Designed to run weekly via scheduled task.
"""

import json
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict

from config import SHOPIFY_BASE_URL, SHOPIFY_ACCESS_TOKEN

logger = logging.getLogger(__name__)

# Reorder calculation defaults
DEFAULT_LEAD_TIME_DAYS = 14
MANUFACTURED_LEAD_TIME_DAYS = 84  # 6-12 weeks for Dr. Ruth's manufactured products
SAFETY_STOCK_MULTIPLIER = 0.20  # 20% buffer above lead time demand
LOOKBACK_DAYS = 90

# Vendors whose products are manufactured (long lead time)
MANUFACTURED_VENDORS = {'Dr. Ruth Roberts'}

# Vendors whose products are virtual bundles (exclude from reorder tracking)
BUNDLE_VENDORS = {'Dr. Ruth Roberts Bundles'}


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


def get_all_inventory_products():
    """
    Pull all products with their variants and inventory info.
    Returns list of dicts with product/variant/inventory details.
    """
    logger.info("Fetching all products from Shopify...")
    products = _shopify_get_all('products.json', 'products', {
        'fields': 'id,title,vendor,product_type,status,variants,tags',
        'limit': 250,
    })

    inventory_items = []
    for product in products:
        for variant in product.get('variants', []):
            if not variant.get('inventory_management'):
                continue  # skip non-tracked variants

            inventory_items.append({
                'product_id': product['id'],
                'variant_id': variant['id'],
                'inventory_item_id': variant.get('inventory_item_id'),
                'product_title': product['title'],
                'variant_title': variant.get('title', 'Default'),
                'sku': variant.get('sku', ''),
                'vendor': product.get('vendor', ''),
                'product_type': product.get('product_type', ''),
                'status': product.get('status', 'active'),
                'price': float(variant.get('price', 0)),
                'inventory_quantity': int(variant.get('inventory_quantity', 0)),
                'tags': product.get('tags', ''),
            })

    # Filter out bundle products (virtual bundles built from individual products)
    inventory_items = [i for i in inventory_items if i['vendor'] not in BUNDLE_VENDORS]

    logger.info(f"Found {len(inventory_items)} inventory-tracked variants across {len(products)} products (bundles excluded)")
    return inventory_items


def get_sales_velocity(lookback_days=LOOKBACK_DAYS):
    """
    Calculate units sold per variant over the lookback period.
    Returns dict of variant_id -> {units_sold, revenue, orders}.
    """
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=lookback_days)
    start_iso = start_date.strftime('%Y-%m-%dT00:00:00Z')
    end_iso = end_date.strftime('%Y-%m-%dT23:59:59Z')

    logger.info(f"Calculating sales velocity from {start_date.date()} to {end_date.date()} ({lookback_days} days)")

    orders = _shopify_get_all('orders.json', 'orders', {
        'status': 'any',
        'financial_status': 'paid',
        'created_at_min': start_iso,
        'created_at_max': end_iso,
        'fields': 'id,line_items,refunds',
    })

    velocity = defaultdict(lambda: {'units_sold': 0, 'revenue': 0.0, 'order_count': 0})

    for order in orders:
        seen_variants = set()
        for item in order.get('line_items', []):
            variant_id = item.get('variant_id')
            if variant_id:
                qty = int(item.get('quantity', 1))
                line_total = float(item.get('price', 0)) * qty
                velocity[variant_id]['units_sold'] += qty
                velocity[variant_id]['revenue'] += line_total
                if variant_id not in seen_variants:
                    velocity[variant_id]['order_count'] += 1
                    seen_variants.add(variant_id)

        # Subtract refunded quantities
        for refund in order.get('refunds', []):
            for refund_line in refund.get('refund_line_items', []):
                variant_id = refund_line.get('line_item', {}).get('variant_id')
                if variant_id:
                    refund_qty = int(refund_line.get('quantity', 0))
                    velocity[variant_id]['units_sold'] -= refund_qty

    logger.info(f"Processed {len(orders)} orders, {len(velocity)} variants had sales")
    return dict(velocity)


def calculate_reorder_points(inventory_items, velocity_data, lookback_days=LOOKBACK_DAYS,
                              lead_time_days=DEFAULT_LEAD_TIME_DAYS,
                              safety_multiplier=SAFETY_STOCK_MULTIPLIER):
    """
    Merge inventory levels with sales velocity to calculate reorder points.

    For each item:
    - avg_daily_sales = units_sold / lookback_days
    - lead_time_demand = avg_daily_sales * lead_time_days
    - safety_stock = lead_time_demand * safety_multiplier
    - reorder_point = lead_time_demand + safety_stock
    - suggested_order_qty = max(reorder_point * 2, min_order_practical)
    - days_of_stock = inventory_quantity / avg_daily_sales (if selling)
    - needs_reorder = True if inventory_quantity <= reorder_point
    """
    results = []

    for item in inventory_items:
        variant_id = item['variant_id']
        vel = velocity_data.get(variant_id, {'units_sold': 0, 'revenue': 0.0, 'order_count': 0})

        # Use longer lead time for manufactured products
        if item['vendor'] in MANUFACTURED_VENDORS:
            effective_lead_time = MANUFACTURED_LEAD_TIME_DAYS
        else:
            effective_lead_time = lead_time_days

        units_sold = max(vel['units_sold'], 0)
        avg_daily_sales = units_sold / lookback_days if lookback_days > 0 else 0
        lead_time_demand = avg_daily_sales * effective_lead_time
        safety_stock = lead_time_demand * safety_multiplier
        reorder_point = lead_time_demand + safety_stock

        # Suggested order: enough for ~90 days for manufactured, ~60 days for resale
        order_cover_days = 90 if item['vendor'] in MANUFACTURED_VENDORS else 60
        suggested_order = max(round(avg_daily_sales * order_cover_days), 2) if units_sold > 0 else 0

        # Days of stock remaining
        days_of_stock = (item['inventory_quantity'] / avg_daily_sales) if avg_daily_sales > 0 else float('inf')

        # Reorder urgency
        needs_reorder = item['inventory_quantity'] <= reorder_point and units_sold > 0

        if days_of_stock == float('inf'):
            urgency = 'No Sales'
        elif days_of_stock <= 0:
            urgency = 'OUT OF STOCK'
        elif days_of_stock <= effective_lead_time:
            urgency = 'URGENT - Order Now'
        elif needs_reorder:
            urgency = 'Reorder Soon'
        else:
            urgency = 'OK'

        results.append({
            **item,
            'units_sold_90d': units_sold,
            'revenue_90d': round(vel['revenue'], 2),
            'order_count_90d': vel['order_count'],
            'avg_daily_sales': round(avg_daily_sales, 4),
            'lead_time_days': effective_lead_time,
            'lead_time_demand': round(lead_time_demand, 2),
            'safety_stock': round(safety_stock, 2),
            'reorder_point': round(reorder_point, 2),
            'suggested_order_qty': suggested_order,
            'days_of_stock': round(days_of_stock, 1) if days_of_stock != float('inf') else 'N/A',
            'needs_reorder': needs_reorder,
            'urgency': urgency,
            'estimated_order_cost': round(suggested_order * item['price'] * 0.6, 2) if suggested_order > 0 else 0,
            # ^^ estimated at ~60% of retail (rough wholesale)
        })

    # Sort: urgent first, then by days_of_stock ascending
    def sort_key(r):
        urgency_order = {
            'OUT OF STOCK': 0, 'URGENT - Order Now': 1,
            'Reorder Soon': 2, 'OK': 3, 'No Sales': 4,
        }
        dos = r['days_of_stock'] if isinstance(r['days_of_stock'], (int, float)) else 99999
        return (urgency_order.get(r['urgency'], 5), dos)

    results.sort(key=sort_key)
    return results


def collect_inventory_data():
    """
    Main entry point: collect all inventory data and return reorder report.
    """
    logger.info("Starting inventory data collection...")

    inventory_items = get_all_inventory_products()
    velocity_data = get_sales_velocity(LOOKBACK_DAYS)
    reorder_report = calculate_reorder_points(inventory_items, velocity_data)

    # Summary stats
    active_items = [r for r in reorder_report if r['status'] == 'active']
    urgent = [r for r in active_items if r['urgency'] in ('OUT OF STOCK', 'URGENT - Order Now')]
    reorder_soon = [r for r in active_items if r['urgency'] == 'Reorder Soon']

    summary = {
        'collection_date': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'lookback_days': LOOKBACK_DAYS,
        'lead_time_days': DEFAULT_LEAD_TIME_DAYS,
        'total_tracked_variants': len(reorder_report),
        'active_variants': len(active_items),
        'urgent_reorder_count': len(urgent),
        'reorder_soon_count': len(reorder_soon),
        'total_estimated_order_cost': round(sum(r['estimated_order_cost'] for r in reorder_report), 2),
    }

    logger.info(f"Inventory collection complete: {summary['active_variants']} active, "
                f"{summary['urgent_reorder_count']} urgent, {summary['reorder_soon_count']} reorder soon")

    return {
        'summary': summary,
        'items': reorder_report,
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_inventory_data()
    print(json.dumps(data['summary'], indent=2))
    print(f"\n--- Top 20 items by urgency ---")
    for item in data['items'][:20]:
        print(f"  {item['urgency']:20s} | Stock: {item['inventory_quantity']:4d} | "
              f"Sold 90d: {item['units_sold_90d']:4d} | "
              f"Reorder@: {item['reorder_point']:6.1f} | {item['product_title'][:50]}")
