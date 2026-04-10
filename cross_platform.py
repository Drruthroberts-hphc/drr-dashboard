"""
Cross-Platform Calculator
=========================
Combines data from all collectors to compute cross-platform KPIs:
total revenue, revenue splits, WoW changes, burn rate, NOI, payroll %,
debt paydown projections, and revenue vs target tracking.
"""

import logging
from datetime import datetime

from config import FINANCIAL

logger = logging.getLogger(__name__)


def calculate_cross_platform(all_data, previous_week_data=None):
    """
    Calculate cross-platform metrics from all collector data.

    Args:
        all_data: dict with keys 'shopify', 'klaviyo', 'stripe', 'ghl', 'social', 'google_ads'
        previous_week_data: dict with previous week's CrossPlatform data (for WoW calc)

    Returns:
        dict with cross-platform metrics
    """
    shopify = all_data.get('shopify') or {}
    klaviyo = all_data.get('klaviyo') or {}
    stripe_data = all_data.get('stripe') or {}
    ghl = all_data.get('ghl') or {}
    social = all_data.get('social') or {}
    google_ads = all_data.get('google_ads') or {}

    # ── Revenue Totals ────────────────────────────────────────────────────
    ecommerce_revenue = float(shopify.get('ecommerce_revenue', 0))
    coaching_revenue = float(shopify.get('coaching_revenue', 0))
    course_revenue = float(shopify.get('course_revenue', 0))
    total_revenue = ecommerce_revenue + coaching_revenue + course_revenue

    # Email-attributed revenue (from Klaviyo) — informational, not additive
    total_email_revenue = float(klaviyo.get('email_attributed_revenue', 0))

    # ── Week-over-Week Change ─────────────────────────────────────────────
    revenue_wow_change = 0.0
    if previous_week_data:
        prev_revenue = float(previous_week_data.get('total_revenue', 0))
        if prev_revenue > 0:
            revenue_wow_change = (total_revenue - prev_revenue) / prev_revenue

    # ── Close Rate (combined) ─────────────────────────────────────────────
    total_close_rate = float(ghl.get('close_rate_overall', 0))

    # ── Financial Health Metrics ──────────────────────────────────────────
    # Monthly burn from verified config (not the old placeholder)
    monthly_burn = FINANCIAL['total_monthly_burn']
    weekly_burn = monthly_burn / 4.33  # Average weeks per month

    # Monthly revenue run rate (annualize the weekly number)
    monthly_revenue_run_rate = total_revenue * 4.33

    # Rana's commission (variable, based on actual coaching sales this week)
    rana_commission = coaching_revenue * FINANCIAL['team']['rana_commission_pct']

    # Team costs: fixed team + Rana commission (weekly)
    weekly_team_cost = (FINANCIAL['team_fixed_total'] / 4.33) + rana_commission

    # Payroll as % of revenue
    payroll_pct = (weekly_team_cost / total_revenue) if total_revenue > 0 else 0

    # NOI (Net Operating Income) — weekly
    noi_weekly = total_revenue - weekly_burn
    noi_margin = (noi_weekly / total_revenue) if total_revenue > 0 else 0

    # Monthly NOI
    noi_monthly = monthly_revenue_run_rate - monthly_burn

    # Cash flow (revenue minus burn minus debt service)
    # Use month-specific debt service if available
    week_ending_str = shopify.get('week_ending_date', '')
    if week_ending_str:
        try:
            debt_month_key = datetime.strptime(week_ending_str, '%Y-%m-%d').strftime('%Y-%m')
            monthly_debt_service = FINANCIAL.get('debt_service_by_month', {}).get(
                debt_month_key, FINANCIAL['monthly_debt_service']
            )
        except ValueError:
            monthly_debt_service = FINANCIAL['monthly_debt_service']
    else:
        monthly_debt_service = FINANCIAL['monthly_debt_service']
    weekly_debt_service = monthly_debt_service / 4.33
    weekly_cash_flow = total_revenue - weekly_burn - weekly_debt_service
    monthly_cash_flow = monthly_revenue_run_rate - monthly_burn - monthly_debt_service

    # Debt paydown
    debt_remaining = FINANCIAL['total_debt']
    if monthly_cash_flow > 0:
        months_to_debt_free = debt_remaining / monthly_cash_flow
    else:
        months_to_debt_free = 999  # Can't pay down debt if cash flow negative

    # Revenue vs target
    week_ending_str = shopify.get('week_ending_date', '')
    revenue_target_monthly = FINANCIAL['revenue_target_monthly']
    if week_ending_str:
        try:
            month_key = datetime.strptime(week_ending_str, '%Y-%m-%d').strftime('%Y-%m')
            revenue_target_monthly = FINANCIAL['revenue_targets_by_month'].get(
                month_key, FINANCIAL['revenue_target_monthly']
            )
        except ValueError:
            pass

    weekly_target = revenue_target_monthly / 4.33
    revenue_vs_target_pct = (total_revenue / weekly_target) if weekly_target > 0 else 0

    # Burn/revenue ratio (for alert: if > 1.0, burning more than earning)
    burn_revenue_ratio = (weekly_burn / total_revenue) if total_revenue > 0 else 999
    # Only flag if it's actually bad (weekly fluctuations are normal)
    burn_rate_exceeds_revenue = burn_revenue_ratio

    # Google Ads metrics for cross-platform view
    ad_spend = float(google_ads.get('ad_spend', 0))
    ad_roas = float(google_ads.get('roas', 0))

    # ── Assemble ──────────────────────────────────────────────────────────
    result = {
        'week_ending_date': week_ending_str,

        # Revenue
        'total_revenue': round(total_revenue, 2),
        'coaching_revenue': round(coaching_revenue, 2),
        'ecommerce_revenue': round(ecommerce_revenue, 2),
        'course_revenue': round(course_revenue, 2),
        'total_email_revenue': round(total_email_revenue, 2),
        'revenue_wow_change': round(revenue_wow_change, 4),

        # Sales
        'total_close_rate': round(total_close_rate, 4),

        # Financial Health — weekly
        'weekly_burn': round(weekly_burn, 2),
        'weekly_team_cost': round(weekly_team_cost, 2),
        'weekly_cash_flow': round(weekly_cash_flow, 2),
        'noi_weekly': round(noi_weekly, 2),
        'noi_margin': round(noi_margin, 4),
        'rana_commission': round(rana_commission, 2),

        # Financial Health — monthly run rates
        'monthly_burn': monthly_burn,
        'monthly_revenue_run_rate': round(monthly_revenue_run_rate, 2),
        'monthly_cash_flow': round(monthly_cash_flow, 2),
        'noi_monthly': round(noi_monthly, 2),

        # Ratios & targets
        'payroll_pct_of_revenue': round(payroll_pct, 4),
        'revenue_vs_target_pct': round(revenue_vs_target_pct, 4),
        'revenue_target_monthly': revenue_target_monthly,
        'weekly_target': round(weekly_target, 2),

        # Debt
        'debt_remaining': debt_remaining,
        'monthly_debt_service': monthly_debt_service,
        'months_to_debt_free': round(months_to_debt_free, 1),

        # Ad performance (cross-platform view)
        'ad_spend_weekly': round(ad_spend, 2),
        'ad_roas': round(ad_roas, 2),

        # Alert-compatible keys
        'burn_rate_exceeds_revenue': round(burn_rate_exceeds_revenue, 4),

        # Legacy compatibility
        'burn_rate': round(weekly_burn, 2),
        'net_profit_loss': round(noi_weekly, 2),
    }

    logger.info(
        f"Cross-platform: revenue ${total_revenue:,.2f} (WoW {revenue_wow_change:+.1%}), "
        f"weekly burn ${weekly_burn:,.2f}, NOI ${noi_weekly:,.2f} ({noi_margin:.1%}), "
        f"payroll {payroll_pct:.1%}, debt ${debt_remaining:,} ({months_to_debt_free:.0f}mo)"
    )

    return result
