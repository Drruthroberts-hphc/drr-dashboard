"""
Cross-Platform Calculator
=========================
Combines data from all collectors to compute cross-platform KPIs:
total revenue, revenue splits, WoW changes, burn rate estimates, etc.
"""

import logging

logger = logging.getLogger(__name__)


def calculate_cross_platform(all_data, previous_week_data=None):
    """
    Calculate cross-platform metrics from all collector data.

    Args:
        all_data: dict with keys 'shopify', 'klaviyo', 'stripe', 'ghl', 'social'
        previous_week_data: dict with previous week's CrossPlatform data (for WoW calc)

    Returns:
        dict with cross-platform metrics
    """
    shopify = all_data.get('shopify') or {}
    klaviyo = all_data.get('klaviyo') or {}
    stripe_data = all_data.get('stripe') or {}
    ghl = all_data.get('ghl') or {}
    social = all_data.get('social') or {}

    # ── Revenue Totals ────────────────────────────────────────────────────
    ecommerce_revenue = float(shopify.get('ecommerce_revenue', 0))
    coaching_revenue = float(shopify.get('coaching_revenue', 0))
    course_revenue = float(shopify.get('course_revenue', 0))
    total_revenue = ecommerce_revenue + coaching_revenue + course_revenue

    # Email-attributed revenue (from Klaviyo)
    total_email_revenue = float(klaviyo.get('email_attributed_revenue', 0))

    # ── Week-over-Week Change ─────────────────────────────────────────────
    revenue_wow_change = 0.0
    if previous_week_data:
        prev_revenue = float(previous_week_data.get('total_revenue', 0))
        if prev_revenue > 0:
            revenue_wow_change = (total_revenue - prev_revenue) / prev_revenue

    # ── Close Rate (combined) ─────────────────────────────────────────────
    total_close_rate = float(ghl.get('close_rate_overall', 0))

    # ── Burn Rate / Net Profit Estimate ───────────────────────────────────
    processing_fees = float(stripe_data.get('processing_fees', 0))
    refund_amount = float(stripe_data.get('refund_amount', 0))
    burn_rate = processing_fees + refund_amount  # Simplified - real burn includes opex
    net_profit_loss = total_revenue - burn_rate

    # ── Assemble ──────────────────────────────────────────────────────────
    result = {
        'week_ending_date': shopify.get('week_ending_date', ''),
        'total_revenue': round(total_revenue, 2),
        'coaching_revenue': round(coaching_revenue, 2),
        'ecommerce_revenue': round(ecommerce_revenue, 2),
        'course_revenue': round(course_revenue, 2),
        'burn_rate': round(burn_rate, 2),
        'net_profit_loss': round(net_profit_loss, 2),
        'total_email_revenue': round(total_email_revenue, 2),
        'total_close_rate': round(total_close_rate, 4),
        'revenue_wow_change': round(revenue_wow_change, 4),
    }

    logger.info(f"Cross-platform: total ${total_revenue:,.2f}, "
                f"WoW {revenue_wow_change:+.1%}, net ${net_profit_loss:,.2f}")

    return result
