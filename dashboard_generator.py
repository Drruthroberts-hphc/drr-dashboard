"""
Dashboard HTML Generator
========================
Generates a single-file interactive HTML dashboard with Chart.js charts,
9 collapsible tiers (including Financial Health and Google Ads), and 70+ KPIs.
Output is suitable for GitHub Pages.
"""

import json
import logging
from datetime import datetime

from config import FINANCIAL

logger = logging.getLogger(__name__)


def _fmt_money(val):
    """Format as currency."""
    try:
        v = float(val)
        if v < 0:
            return f"-${abs(v):,.2f}"
        return f"${v:,.2f}"
    except (ValueError, TypeError):
        return "$0.00"


def _fmt_pct(val, decimals=1):
    """Format as percentage."""
    try:
        v = float(val)
        return f"{v * 100:.{decimals}f}%"
    except (ValueError, TypeError):
        return "0.0%"


def _fmt_int(val):
    """Format as integer with commas."""
    try:
        return f"{int(float(val)):,}"
    except (ValueError, TypeError):
        return "0"


def _fmt_float(val, decimals=1):
    """Format as float."""
    try:
        return f"{float(val):,.{decimals}f}"
    except (ValueError, TypeError):
        return "0.0"


def _wow_badge(current, previous, is_inverse=False):
    """Generate a WoW change badge HTML."""
    try:
        c = float(current)
        p = float(previous)
    except (ValueError, TypeError):
        return '<span class="badge badge-neutral">--</span>'

    if p == 0:
        return '<span class="badge badge-neutral">NEW</span>'

    change = (c - p) / abs(p)
    pct = f"{change:+.1%}"

    if is_inverse:
        css = "badge-red" if change > 0 else "badge-green" if change < 0 else "badge-neutral"
    else:
        css = "badge-green" if change > 0 else "badge-red" if change < 0 else "badge-neutral"

    arrow = "&#9650;" if change > 0 else "&#9660;" if change < 0 else "&#9644;"
    return f'<span class="badge {css}">{arrow} {pct}</span>'


def _metric_card(label, value, wow_html="", subtitle="", color=None):
    """Generate a metric card HTML snippet."""
    sub = f'<div class="card-subtitle">{subtitle}</div>' if subtitle else ""
    wow = f'<div class="card-wow">{wow_html}</div>' if wow_html else ""
    val_style = f' style="color: {color};"' if color else ""
    return f"""
    <div class="metric-card">
        <div class="card-label">{label}</div>
        <div class="card-value"{val_style}>{value}</div>
        {wow}
        {sub}
    </div>"""


def _progress_bar(label, current, target, color="#2e7d32", show_pct=True):
    """Generate a progress bar HTML snippet."""
    try:
        pct = min((float(current) / float(target)) * 100, 100) if float(target) > 0 else 0
    except (ValueError, TypeError):
        pct = 0
    pct_label = f" ({pct:.0f}%)" if show_pct else ""
    bar_color = color if pct < 100 else "#2e7d32"
    return f"""
    <div class="progress-row">
        <div class="progress-label">{label}{pct_label}</div>
        <div class="progress-track">
            <div class="progress-fill" style="width:{pct:.0f}%; background:{bar_color};"></div>
        </div>
    </div>"""


def _build_trend_data(history):
    """
    Extract time-series arrays from snapshot history for Chart.js.
    Returns a dict ready for JSON serialization with labels and datasets.
    """
    if not history:
        return {}

    labels = []

    # Revenue
    total_revenue = []
    ecommerce_revenue = []
    coaching_revenue = []
    course_revenue = []

    # Email
    email_revenue = []
    open_rate = []
    click_rate = []
    list_size = []

    # Shopify
    order_count = []
    aov = []
    new_customers = []

    # Stripe
    mrr = []
    gross_volume = []

    # Social
    yt_subscribers = []
    fb_followers = []
    ig_followers = []
    ig_engagement = []

    # Pipeline
    new_leads = []
    booked = []
    active_students = []

    # Financial
    noi_weekly = []
    payroll_pct = []
    weekly_burn = []

    # Google Ads
    ad_spend = []
    ad_roas = []

    for snap in history:
        wed = snap.get('week_ending_date', '')
        try:
            d = datetime.strptime(wed, '%Y-%m-%d')
            labels.append(f"{d.month}/{d.day}")
        except Exception:
            labels.append(wed[-5:] if len(wed) >= 5 else wed)

        ad = snap.get('all_data', {})
        cp = snap.get('cross_platform', ad.get('cross_platform', {}))
        sh = ad.get('shopify', {})
        kl = ad.get('klaviyo', {})
        st = ad.get('stripe', {})
        gh = ad.get('ghl', {})
        so = ad.get('social', {})
        ga = ad.get('google_ads', {})

        # Revenue
        total_revenue.append(float(cp.get('total_revenue', 0)))
        ecommerce_revenue.append(float(cp.get('ecommerce_revenue', 0)))
        coaching_revenue.append(float(cp.get('coaching_revenue', 0)))
        course_revenue.append(float(cp.get('course_revenue', 0)))

        # Email
        email_revenue.append(float(kl.get('email_attributed_revenue', 0)))
        open_rate.append(round(float(kl.get('open_rate', 0)) * 100, 1))
        click_rate.append(round(float(kl.get('click_rate', 0)) * 100, 2))
        list_size.append(int(kl.get('list_size', 0)))

        # Shopify
        order_count.append(int(sh.get('order_count', 0)))
        aov.append(float(sh.get('aov', 0)))
        new_customers.append(int(sh.get('new_customers', 0)))

        # Stripe
        mrr.append(float(st.get('mrr', 0)))
        gross_volume.append(float(st.get('gross_payment_volume', 0)))

        # Social
        yt_subscribers.append(int(so.get('yt_subscribers', 0)))
        fb_followers.append(int(so.get('fb_followers', 0)))
        ig_followers.append(int(so.get('ig_followers', 0)))
        ig_engagement.append(round(float(so.get('ig_engagement_rate', 0)) * 100, 2))

        # Pipeline
        new_leads.append(int(gh.get('new_leads', 0)))
        booked.append(int(gh.get('booked_appointments', 0)))
        active_students.append(int(gh.get('active_students', 0)))

        # Financial
        noi_weekly.append(float(cp.get('noi_weekly', 0)))
        payroll_pct.append(round(float(cp.get('payroll_pct_of_revenue', 0)) * 100, 1))
        weekly_burn.append(float(cp.get('weekly_burn', 0)))

        # Google Ads
        ad_spend.append(float(ga.get('ad_spend', 0)))
        ad_roas.append(float(ga.get('roas', 0)))

    return {
        'labels': labels,
        'total_revenue': total_revenue,
        'ecommerce_revenue': ecommerce_revenue,
        'coaching_revenue': coaching_revenue,
        'course_revenue': course_revenue,
        'email_revenue': email_revenue,
        'open_rate': open_rate,
        'click_rate': click_rate,
        'list_size': list_size,
        'order_count': order_count,
        'aov': aov,
        'new_customers': new_customers,
        'mrr': mrr,
        'gross_volume': gross_volume,
        'yt_subscribers': yt_subscribers,
        'fb_followers': fb_followers,
        'ig_followers': ig_followers,
        'ig_engagement': ig_engagement,
        'new_leads': new_leads,
        'booked': booked,
        'active_students': active_students,
        'noi_weekly': noi_weekly,
        'payroll_pct': payroll_pct,
        'weekly_burn': weekly_burn,
        'ad_spend': ad_spend,
        'ad_roas': ad_roas,
    }


def generate_dashboard(all_data, cross_data, alerts, previous_data=None, week_ending_date=None, history=None):
    """
    Generate the full single-file HTML dashboard.

    Args:
        all_data: dict with 'shopify', 'klaviyo', 'stripe', 'ghl', 'social', 'google_ads' data
        cross_data: dict with cross-platform metrics
        alerts: list of triggered alerts
        previous_data: dict with previous week's data (same structure as all_data)
        week_ending_date: string date for the week
        history: list of snapshot dicts for trend charts

    Returns:
        str: Complete HTML document
    """
    if week_ending_date is None:
        week_ending_date = cross_data.get('week_ending_date', datetime.utcnow().strftime('%Y-%m-%d'))

    shopify = all_data.get('shopify') or {}
    klaviyo = all_data.get('klaviyo') or {}
    stripe_data = all_data.get('stripe') or {}
    ghl = all_data.get('ghl') or {}
    social = all_data.get('social') or {}
    google_ads = all_data.get('google_ads') or {}

    prev = previous_data or {}
    prev_shopify = prev.get('shopify') or {}
    prev_klaviyo = prev.get('klaviyo') or {}
    prev_stripe = prev.get('stripe') or {}
    prev_ghl = prev.get('ghl') or {}
    prev_social = prev.get('social') or {}
    prev_cross = prev.get('cross_platform') or {}
    prev_google_ads = prev.get('google_ads') or {}

    # Alert banner
    alert_banner = ""
    if alerts:
        items = "".join(
            f'<li><strong>{a["metric_name"]}</strong>: {a["current_value"]:.4f} '
            f'({a["direction"]} {a["threshold"]})</li>'
            for a in alerts
        )
        alert_banner = f"""
        <div class="alert-banner">
            <h3>&#9888; {len(alerts)} Alert(s) Triggered</h3>
            <ul>{items}</ul>
        </div>"""

    # ── Revenue vs Target gauge ───────────────────────────────────────────
    rev_target_pct = float(cross_data.get('revenue_vs_target_pct', 0))
    weekly_target = float(cross_data.get('weekly_target', 0))
    rev_target_color = "#2e7d32" if rev_target_pct >= 0.9 else "#e65100" if rev_target_pct >= 0.7 else "#c62828"

    # NOI color
    noi_val = float(cross_data.get('noi_weekly', 0))
    noi_color = "#2e7d32" if noi_val >= 0 else "#c62828"

    # Cash flow color
    cf_val = float(cross_data.get('weekly_cash_flow', 0))
    cf_color = "#2e7d32" if cf_val >= 0 else "#c62828"

    # ── Tier 1: Executive Overview (enhanced) ─────────────────────────────
    tier1 = f"""
    <div class="tier" id="tier1">
        <div class="tier-header" onclick="toggleTier('tier1-body')">
            <h2>&#9656; Tier 1: Executive Overview</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier1-body">
            {alert_banner}
            <div class="card-grid">
                {_metric_card("Total Revenue", _fmt_money(cross_data.get('total_revenue', 0)),
                    _wow_badge(cross_data.get('total_revenue',0), prev_cross.get('total_revenue',0)))}
                {_metric_card("E-Commerce", _fmt_money(cross_data.get('ecommerce_revenue', 0)),
                    _wow_badge(cross_data.get('ecommerce_revenue',0), prev_cross.get('ecommerce_revenue',0)))}
                {_metric_card("Coaching", _fmt_money(cross_data.get('coaching_revenue', 0)),
                    _wow_badge(cross_data.get('coaching_revenue',0), prev_cross.get('coaching_revenue',0)))}
                {_metric_card("Courses", _fmt_money(cross_data.get('course_revenue', 0)),
                    _wow_badge(cross_data.get('course_revenue',0), prev_cross.get('course_revenue',0)))}
                {_metric_card("NOI (Weekly)", _fmt_money(noi_val), color=noi_color,
                    subtitle=f"Margin: {_fmt_pct(cross_data.get('noi_margin', 0))}")}
                {_metric_card("Cash Flow (Weekly)", _fmt_money(cf_val), color=cf_color)}
                {_metric_card("Monthly Burn", _fmt_money(cross_data.get('monthly_burn', 0)),
                    subtitle="Verified from bank data")}
                {_metric_card("Active Students", _fmt_int(ghl.get('active_students', 0)))}
            </div>

            <h3>Revenue vs Target</h3>
            {_progress_bar(
                f"Weekly: {_fmt_money(cross_data.get('total_revenue',0))} / {_fmt_money(weekly_target)}",
                cross_data.get('total_revenue', 0), weekly_target, color=rev_target_color
            )}
            {_progress_bar(
                f"Monthly run rate: {_fmt_money(cross_data.get('monthly_revenue_run_rate',0))} / {_fmt_money(cross_data.get('revenue_target_monthly',0))}",
                cross_data.get('monthly_revenue_run_rate', 0), cross_data.get('revenue_target_monthly', 0),
                color=rev_target_color
            )}

            <div class="chart-container">
                <canvas id="revenueChart" height="250"></canvas>
            </div>
        </div>
    </div>"""

    # ── Tier 2: Financial Health ──────────────────────────────────────────
    payroll_pct = float(cross_data.get('payroll_pct_of_revenue', 0))
    payroll_color = "#2e7d32" if payroll_pct < 0.25 else "#e65100" if payroll_pct < 0.30 else "#c62828"

    debt_remaining = float(cross_data.get('debt_remaining', 0))
    months_dtf = float(cross_data.get('months_to_debt_free', 999))

    # Build expense breakdown table
    expense_rows = ""
    categories = [
        ("Team (fixed)", FINANCIAL['team_fixed_total']),
        ("Rana Commission (est.)", float(cross_data.get('rana_commission', 0)) * 4.33),
        ("Google Ads", FINANCIAL['google_ads_budget']),
        ("COGS & Shipping", FINANCIAL['cogs_standard_process'] + FINANCIAL['cogs_other'] + FINANCIAL['healthy_life_shipping']),
        ("Mentors (est.)", FINANCIAL['mentor_monthly_est']),
        ("Payment Processing", FINANCIAL['stripe_processing_est'] + FINANCIAL['shopify_fees_est'] + FINANCIAL['paypal_fees_est']),
        ("Software & Services", FINANCIAL['services_total']),
        ("Debt Service", FINANCIAL['monthly_debt_service']),
    ]
    for cat_name, cat_amount in categories:
        expense_rows += f'<tr><td>{cat_name}</td><td class="text-right">{_fmt_money(cat_amount)}</td></tr>'

    tier2 = f"""
    <div class="tier" id="tier2">
        <div class="tier-header" onclick="toggleTier('tier2-body')">
            <h2>&#9656; Tier 2: Financial Health</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier2-body">
            <div class="card-grid">
                {_metric_card("Payroll % of Revenue", _fmt_pct(payroll_pct), color=payroll_color,
                    subtitle="Target: < 25%")}
                {_metric_card("NOI Margin", _fmt_pct(cross_data.get('noi_margin', 0)),
                    color=noi_color)}
                {_metric_card("Monthly Cash Flow", _fmt_money(cross_data.get('monthly_cash_flow', 0)),
                    color=cf_color, subtitle="After debt service")}
                {_metric_card("Monthly Revenue Run Rate", _fmt_money(cross_data.get('monthly_revenue_run_rate', 0)))}
                {_metric_card("Debt Remaining", _fmt_money(debt_remaining),
                    subtitle=f"~{months_dtf:.0f} months to debt-free" if months_dtf < 900 else "Cash flow negative")}
                {_metric_card("Monthly Burn", _fmt_money(cross_data.get('monthly_burn', 0)))}
                {_metric_card("Ad Spend (Weekly)", _fmt_money(cross_data.get('ad_spend_weekly', 0)),
                    subtitle=f"ROAS: {cross_data.get('ad_roas', 0):.1f}x")}
                {_metric_card("Email Revenue", _fmt_money(cross_data.get('total_email_revenue', 0)))}
            </div>

            <h3>Debt Paydown Progress</h3>
            {_progress_bar(
                f"Paid: {_fmt_money(FINANCIAL['total_debt'] - debt_remaining)} of {_fmt_money(FINANCIAL['total_debt'])}",
                FINANCIAL['total_debt'] - debt_remaining, FINANCIAL['total_debt'], color="#1565c0"
            )}

            <h3>Monthly Expense Breakdown</h3>
            <table class="data-table">
                <thead><tr><th>Category</th><th class="text-right">Monthly</th></tr></thead>
                <tbody>
                    {expense_rows}
                    <tr style="font-weight: bold; border-top: 2px solid #333;">
                        <td>Total Monthly Burn</td>
                        <td class="text-right">{_fmt_money(FINANCIAL['total_monthly_burn'])}</td>
                    </tr>
                </tbody>
            </table>

            <div class="chart-container">
                <canvas id="expenseChart" height="280"></canvas>
            </div>
        </div>
    </div>"""

    # ── Tier 3: Google Ads ────────────────────────────────────────────────
    ga_spend = float(google_ads.get('ad_spend', 0))
    ga_roas = float(google_ads.get('roas', 0))
    ga_disapproval = float(google_ads.get('disapproval_rate', 0))
    roas_color = "#2e7d32" if ga_roas >= 3.0 else "#e65100" if ga_roas >= 2.0 else "#c62828"
    disapproval_color = "#c62828" if ga_disapproval > 0.20 else "#e65100" if ga_disapproval > 0.10 else "#2e7d32"

    tier3 = f"""
    <div class="tier" id="tier3">
        <div class="tier-header" onclick="toggleTier('tier3-body')">
            <h2>&#9656; Tier 3: Google Ads</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier3-body">
            <div class="card-grid">
                {_metric_card("Ad Spend", _fmt_money(ga_spend),
                    _wow_badge(ga_spend, float(prev_google_ads.get('ad_spend', 0))))}
                {_metric_card("ROAS", f"{ga_roas:.1f}x", color=roas_color,
                    subtitle="Target: > 3.0x")}
                {_metric_card("Conversion Value", _fmt_money(google_ads.get('conversion_value', 0)))}
                {_metric_card("Conversions", _fmt_float(google_ads.get('conversions', 0)))}
                {_metric_card("CPA", _fmt_money(google_ads.get('cpa', 0)))}
                {_metric_card("Clicks", _fmt_int(google_ads.get('clicks', 0)))}
                {_metric_card("Impressions", _fmt_int(google_ads.get('impressions', 0)))}
                {_metric_card("CTR", _fmt_pct(google_ads.get('ctr', 0)))}
            </div>

            <h3>Shopping Product Health</h3>
            <div class="card-grid">
                {_metric_card("Active Products", _fmt_int(google_ads.get('active_products', 0)),
                    color="#2e7d32")}
                {_metric_card("Disapproved Products", _fmt_int(google_ads.get('disapproved_products', 0)),
                    color=disapproval_color)}
                {_metric_card("Disapproval Rate", _fmt_pct(ga_disapproval),
                    color=disapproval_color,
                    subtitle="45% = lost revenue at 5.1x ROAS" if ga_disapproval > 0.40 else "Target: < 10%")}
            </div>

            <div class="chart-row">
                <div class="chart-half"><canvas id="adsSpendROAS" height="280"></canvas></div>
                <div class="chart-half"><canvas id="adsProductHealth" height="280"></canvas></div>
            </div>
        </div>
    </div>"""

    # ── Tier 4: Sales Pipeline (GHL) ──────────────────────────────────────
    tier4 = f"""
    <div class="tier" id="tier4">
        <div class="tier-header" onclick="toggleTier('tier4-body')">
            <h2>&#9656; Tier 4: Sales Pipeline (GHL)</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier4-body">
            <div class="card-grid">
                {_metric_card("New Leads", _fmt_int(ghl.get('new_leads', 0)),
                    _wow_badge(ghl.get('new_leads',0), prev_ghl.get('new_leads',0)))}
                {_metric_card("Booked Calls", _fmt_int(ghl.get('booked_appointments', 0)),
                    _wow_badge(ghl.get('booked_appointments',0), prev_ghl.get('booked_appointments',0)))}
                {_metric_card("Showed", _fmt_int(ghl.get('showed_appointments', 0)))}
                {_metric_card("Closed Deals", _fmt_int(ghl.get('closed_deals', 0)))}
                {_metric_card("Close Rate (Overall)", _fmt_pct(ghl.get('close_rate_overall', 0)),
                    _wow_badge(ghl.get('close_rate_overall',0), prev_ghl.get('close_rate_overall',0)))}
                {_metric_card("Close Rate (Rana)", _fmt_pct(ghl.get('close_rate_rana', 0)))}
                {_metric_card("Pipeline Value", _fmt_money(ghl.get('pipeline_value', 0)))}
                {_metric_card("Revenue per Call", _fmt_money(ghl.get('revenue_per_call', 0)))}
            </div>
            <div class="chart-container">
                <canvas id="funnelChart" height="250"></canvas>
            </div>
        </div>
    </div>"""

    # ── Tier 5: Email Marketing (Klaviyo) ─────────────────────────────────
    tier5 = f"""
    <div class="tier" id="tier5">
        <div class="tier-header" onclick="toggleTier('tier5-body')">
            <h2>&#9656; Tier 5: Email Marketing (Klaviyo)</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier5-body">
            <div class="card-grid">
                {_metric_card("Email Revenue", _fmt_money(klaviyo.get('email_attributed_revenue', 0)),
                    _wow_badge(klaviyo.get('email_attributed_revenue',0), prev_klaviyo.get('email_attributed_revenue',0)))}
                {_metric_card("Open Rate", _fmt_pct(klaviyo.get('open_rate', 0)),
                    _wow_badge(klaviyo.get('open_rate',0), prev_klaviyo.get('open_rate',0)))}
                {_metric_card("Click Rate", _fmt_pct(klaviyo.get('click_rate', 0)),
                    _wow_badge(klaviyo.get('click_rate',0), prev_klaviyo.get('click_rate',0)))}
                {_metric_card("Click-to-Open", _fmt_pct(klaviyo.get('ctor', 0)))}
                {_metric_card("List Size", _fmt_int(klaviyo.get('list_size', 0)),
                    _wow_badge(klaviyo.get('list_size',0), prev_klaviyo.get('list_size',0)))}
                {_metric_card("Delivery Rate", _fmt_pct(klaviyo.get('delivery_rate', 0)))}
                {_metric_card("Spam Rate", _fmt_pct(klaviyo.get('spam_complaint_rate', 0), 3),
                    subtitle="Target: < 0.1%")}
                {_metric_card("Welcome Flow Rev", _fmt_money(klaviyo.get('welcome_flow_revenue', 0)))}
            </div>
        </div>
    </div>"""

    # ── Tier 6: E-Commerce Detail (Shopify) ───────────────────────────────
    top_products_html = ""
    try:
        top_products = json.loads(shopify.get('top_products_json', '[]'))
        if top_products:
            rows = "".join(
                f'<tr><td>{p.get("name", p.get("title",""))[:40]}</td>'
                f'<td class="text-right">{_fmt_money(p.get("revenue",0))}</td>'
                f'<td class="text-right">{_fmt_int(p.get("quantity",0))}</td></tr>'
                for p in top_products[:5]
            )
            top_products_html = f"""
            <h3>Top 5 Products</h3>
            <table class="data-table">
                <thead><tr><th>Product</th><th class="text-right">Revenue</th><th class="text-right">Qty</th></tr></thead>
                <tbody>{rows}</tbody>
            </table>"""
    except (json.JSONDecodeError, TypeError):
        pass

    tier6 = f"""
    <div class="tier" id="tier6">
        <div class="tier-header" onclick="toggleTier('tier6-body')">
            <h2>&#9656; Tier 6: E-Commerce Detail (Shopify)</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier6-body">
            <div class="card-grid">
                {_metric_card("Gross Revenue", _fmt_money(shopify.get('gross_revenue', 0)),
                    _wow_badge(shopify.get('gross_revenue',0), prev_shopify.get('gross_revenue',0)))}
                {_metric_card("Net Revenue", _fmt_money(shopify.get('net_revenue', 0)))}
                {_metric_card("Orders", _fmt_int(shopify.get('order_count', 0)),
                    _wow_badge(shopify.get('order_count',0), prev_shopify.get('order_count',0)))}
                {_metric_card("AOV", _fmt_money(shopify.get('aov', 0)),
                    _wow_badge(shopify.get('aov',0), prev_shopify.get('aov',0)))}
                {_metric_card("New Customers", _fmt_int(shopify.get('new_customers', 0)))}
                {_metric_card("Returning Customers", _fmt_int(shopify.get('returning_customers', 0)))}
                {_metric_card("Conversion Rate", _fmt_pct(shopify.get('conversion_rate', 0)))}
                {_metric_card("Return Rate", _fmt_pct(shopify.get('return_rate', 0)),
                    subtitle="Target: < 5%")}
            </div>
            {top_products_html}
        </div>
    </div>"""

    # ── Tier 7: Payments & Health (Stripe) ────────────────────────────────
    tier7 = f"""
    <div class="tier" id="tier7">
        <div class="tier-header" onclick="toggleTier('tier7-body')">
            <h2>&#9656; Tier 7: Payments &amp; Health (Stripe)</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier7-body">
            <div class="card-grid">
                {_metric_card("Gross Volume", _fmt_money(stripe_data.get('gross_payment_volume', 0)),
                    _wow_badge(stripe_data.get('gross_payment_volume',0), prev_stripe.get('gross_payment_volume',0)))}
                {_metric_card("Net Revenue", _fmt_money(stripe_data.get('net_revenue', 0)))}
                {_metric_card("Payment Success", _fmt_pct(stripe_data.get('payment_success_rate', 0)),
                    subtitle="Target: > 95%")}
                {_metric_card("Dispute Rate", _fmt_pct(stripe_data.get('dispute_rate', 0), 3),
                    subtitle="Target: < 0.5%")}
                {_metric_card("Refund Rate", _fmt_pct(stripe_data.get('refund_rate', 0)))}
                {_metric_card("Refund Amount", _fmt_money(stripe_data.get('refund_amount', 0)))}
                {_metric_card("MRR", _fmt_money(stripe_data.get('mrr', 0)),
                    _wow_badge(stripe_data.get('mrr',0), prev_stripe.get('mrr',0)))}
                {_metric_card("Processing Fees", _fmt_money(stripe_data.get('processing_fees', 0)))}
            </div>
        </div>
    </div>"""

    # ── Tier 8: Coaching Program ──────────────────────────────────────────
    tier8 = f"""
    <div class="tier" id="tier8">
        <div class="tier-header" onclick="toggleTier('tier8-body')">
            <h2>&#9656; Tier 8: Coaching Program</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier8-body">
            <div class="card-grid">
                {_metric_card("Active Students", _fmt_int(ghl.get('active_students', 0)),
                    _wow_badge(ghl.get('active_students',0), prev_ghl.get('active_students',0)))}
                {_metric_card("Enrollment Growth", _fmt_pct(ghl.get('enrollment_growth_rate', 0)))}
                {_metric_card("Student Churn", _fmt_pct(ghl.get('student_churn_rate', 0)),
                    subtitle="Target: < 15%")}
                {_metric_card("Rev per Student", _fmt_money(ghl.get('revenue_per_student', 0)))}
                {_metric_card("Rana Commission", _fmt_money(cross_data.get('rana_commission', 0)),
                    subtitle="10% of coaching revenue")}
                {_metric_card("Mentor Cost (est.)", _fmt_money(FINANCIAL['mentor_monthly_est'] / 4.33),
                    subtitle=f"${FINANCIAL['mentor_cost_per_student']}/student")}
            </div>
        </div>
    </div>"""

    # ── Tier 9: Social Media ──────────────────────────────────────────────
    yt_total_views = social.get('yt_total_views', 0)
    prev_yt_total_views = prev_social.get('yt_total_views', 0)
    yt_weekly_views = max(0, yt_total_views - prev_yt_total_views) if prev_yt_total_views else social.get('yt_views', 0)
    yt_avg_watch_min = social.get('yt_avg_watch_min', 0)
    yt_watch_hours = social.get('yt_watch_hours', 0)
    if yt_watch_hours == 0 and yt_weekly_views > 0 and yt_avg_watch_min > 0:
        yt_watch_hours = round(yt_weekly_views * yt_avg_watch_min / 60, 1)

    prev_total_comments = prev_social.get('yt_total_comments', 0)
    cur_total_comments = social.get('yt_total_comments', 0)
    yt_weekly_comments = social.get('yt_comments', 0)
    if cur_total_comments and prev_total_comments:
        delta_comments = max(0, cur_total_comments - prev_total_comments)
        if delta_comments > 0:
            yt_weekly_comments = delta_comments

    # YouTube recent videos table
    yt_videos_html = ""
    try:
        yt_recent_videos = json.loads(social.get('yt_recent_videos_json', '[]'))
        if yt_recent_videos:
            yt_rows = "".join(
                f'<tr><td>{v.get("title","")[:50]}</td>'
                f'<td>{v.get("published","")}</td>'
                f'<td class="text-right">{_fmt_int(v.get("views",0))}</td>'
                f'<td class="text-right">{_fmt_int(v.get("likes",0))}</td>'
                f'<td class="text-right">{_fmt_int(v.get("comments",0))}</td>'
                f'<td class="text-right">{v.get("duration_min",0)} min</td></tr>'
                for v in yt_recent_videos[:5]
            )
            yt_videos_html = f"""
            <h4>Recent Videos</h4>
            <table class="data-table">
                <thead><tr><th>Title</th><th>Published</th><th class="text-right">Views</th>
                <th class="text-right">Likes</th><th class="text-right">Comments</th>
                <th class="text-right">Duration</th></tr></thead>
                <tbody>{yt_rows}</tbody>
            </table>"""
    except (json.JSONDecodeError, TypeError):
        pass

    # Facebook top posts table
    fb_posts_html = ""
    try:
        fb_top_posts = json.loads(social.get('fb_top_posts_json', '[]'))
        if fb_top_posts:
            def _fb_post_cell(p):
                txt = p.get("message", "")[:50]
                link = p.get("link", "")
                if link:
                    return f'<a href="{link}" target="_blank">{txt}</a>'
                return txt
            fb_rows = "".join(
                f'<tr><td>{_fb_post_cell(p)}</td>'
                f'<td>{p.get("date","")}</td>'
                f'<td class="text-right">{_fmt_int(p.get("reactions",0))}</td>'
                f'<td class="text-right">{_fmt_int(p.get("comments",0))}</td>'
                f'<td class="text-right">{_fmt_int(p.get("shares",0))}</td>'
                f'<td class="text-right">{_fmt_int(p.get("total_engagement",0))}</td></tr>'
                for p in fb_top_posts[:5]
            )
            fb_posts_html = f"""
            <h4>Top Facebook Posts This Week</h4>
            <table class="data-table">
                <thead><tr><th>Post</th><th>Date</th><th class="text-right">Reactions</th>
                <th class="text-right">Comments</th><th class="text-right">Shares</th>
                <th class="text-right">Total</th></tr></thead>
                <tbody>{fb_rows}</tbody>
            </table>"""
    except (json.JSONDecodeError, TypeError):
        pass

    # Instagram top posts table
    ig_posts_html = ""
    try:
        ig_top_posts = json.loads(social.get('ig_top_posts_json', '[]'))
        if ig_top_posts:
            def _ig_post_cell(p):
                txt = p.get("caption", "")[:50]
                link = p.get("link", "")
                if link:
                    return f'<a href="{link}" target="_blank">{txt}</a>'
                return txt
            ig_rows = "".join(
                f'<tr><td>{_ig_post_cell(p)}</td>'
                f'<td>{p.get("date","")}</td>'
                f'<td>{p.get("media_type","")}</td>'
                f'<td class="text-right">{_fmt_int(p.get("likes",0))}</td>'
                f'<td class="text-right">{_fmt_int(p.get("comments",0))}</td>'
                f'<td class="text-right">{_fmt_int(p.get("total_engagement",0))}</td></tr>'
                for p in ig_top_posts[:5]
            )
            ig_posts_html = f"""
            <h4>Top Instagram Posts This Week</h4>
            <table class="data-table">
                <thead><tr><th>Caption</th><th>Date</th><th>Type</th>
                <th class="text-right">Likes</th><th class="text-right">Comments</th>
                <th class="text-right">Total</th></tr></thead>
                <tbody>{ig_rows}</tbody>
            </table>"""
    except (json.JSONDecodeError, TypeError):
        pass

    tier9 = f"""
    <div class="tier" id="tier9">
        <div class="tier-header" onclick="toggleTier('tier9-body')">
            <h2>&#9656; Tier 9: Social Media</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier9-body">
            <h3>YouTube</h3>
            <div class="card-grid">
                {_metric_card("Subscribers", _fmt_int(social.get('yt_subscribers', 0)),
                    _wow_badge(social.get('yt_subscribers',0), prev_social.get('yt_subscribers',0)))}
                {_metric_card("Total Views", _fmt_int(yt_total_views),
                    _wow_badge(yt_total_views, prev_yt_total_views) if prev_yt_total_views else '')}
                {_metric_card("Views This Week", _fmt_int(yt_weekly_views))}
                {_metric_card("Watch Hours (est.)", _fmt_float(yt_watch_hours))}
                {_metric_card("New Videos", _fmt_int(social.get('yt_new_videos', 0)))}
                {_metric_card("Comments", _fmt_int(yt_weekly_comments))}
            </div>
            {yt_videos_html}

            <h3>Facebook</h3>
            <div class="card-grid">
                {_metric_card("Followers", _fmt_int(social.get('fb_followers', 0)),
                    _wow_badge(social.get('fb_followers',0), prev_social.get('fb_followers',0)))}
                {_metric_card("Reach", _fmt_int(social.get('fb_reach', 0)))}
                {_metric_card("Engagement Rate", _fmt_pct(social.get('fb_engagement_rate', 0)))}
                {_metric_card("Likes", _fmt_int(social.get('fb_week_likes', 0)))}
                {_metric_card("Comments", _fmt_int(social.get('fb_week_comments', 0)))}
                {_metric_card("Shares", _fmt_int(social.get('fb_week_shares', 0)))}
            </div>
            {fb_posts_html}

            <h3>Instagram</h3>
            <div class="card-grid">
                {_metric_card("Followers", _fmt_int(social.get('ig_followers', 0)),
                    _wow_badge(social.get('ig_followers',0), prev_social.get('ig_followers',0)))}
                {_metric_card("Engagement Rate", _fmt_pct(social.get('ig_engagement_rate', 0)))}
                {_metric_card("Likes", _fmt_int(social.get('ig_week_likes', 0)))}
                {_metric_card("Comments", _fmt_int(social.get('ig_week_comments', 0)))}
            </div>
            {ig_posts_html}
        </div>
    </div>"""

    # ── Tier 10: Trends ───────────────────────────────────────────────────
    trend_data = _build_trend_data(history or [])
    has_trends = bool(trend_data.get('labels'))

    tier10 = ""
    if has_trends:
        tier10 = """
    <div class="tier" id="tier10">
        <div class="tier-header" onclick="toggleTier('tier10-body')">
            <h2>&#9656; Trends &amp; Year-over-Year</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier10-body">
            <h3>Revenue Trends</h3>
            <div class="chart-row">
                <div class="chart-half"><canvas id="trendRevenue" height="280"></canvas></div>
                <div class="chart-half"><canvas id="trendRevBySilo" height="280"></canvas></div>
            </div>

            <h3>Financial Health Trends</h3>
            <div class="chart-row">
                <div class="chart-half"><canvas id="trendNOI" height="280"></canvas></div>
                <div class="chart-half"><canvas id="trendPayroll" height="280"></canvas></div>
            </div>

            <h3>Google Ads Trends</h3>
            <div class="chart-row">
                <div class="chart-half"><canvas id="trendAdSpend" height="280"></canvas></div>
                <div class="chart-half"><canvas id="trendAdROAS" height="280"></canvas></div>
            </div>

            <h3>Email Marketing Trends</h3>
            <div class="chart-row">
                <div class="chart-half"><canvas id="trendEmailRev" height="280"></canvas></div>
                <div class="chart-half"><canvas id="trendEmailRates" height="280"></canvas></div>
            </div>

            <h3>E-Commerce Trends</h3>
            <div class="chart-row">
                <div class="chart-half"><canvas id="trendOrders" height="280"></canvas></div>
                <div class="chart-half"><canvas id="trendAOV" height="280"></canvas></div>
            </div>

            <h3>Payments &amp; Subscriptions</h3>
            <div class="chart-row">
                <div class="chart-half"><canvas id="trendMRR" height="280"></canvas></div>
                <div class="chart-half"><canvas id="trendStripeVol" height="280"></canvas></div>
            </div>

            <h3>Social Media Growth</h3>
            <div class="chart-row">
                <div class="chart-half"><canvas id="trendSocial" height="280"></canvas></div>
                <div class="chart-half"><canvas id="trendYT" height="280"></canvas></div>
            </div>

            <h3>Sales Pipeline &amp; Coaching</h3>
            <div class="chart-row">
                <div class="chart-half"><canvas id="trendPipeline" height="280"></canvas></div>
                <div class="chart-half"><canvas id="trendStudents" height="280"></canvas></div>
            </div>
        </div>
    </div>"""

    # ── Chart Data ────────────────────────────────────────────────────────
    chart_data = {
        "revenue": {
            "ecommerce": float(cross_data.get('ecommerce_revenue', 0)),
            "coaching": float(cross_data.get('coaching_revenue', 0)),
            "courses": float(cross_data.get('course_revenue', 0)),
        },
        "funnel": {
            "leads": int(ghl.get('new_leads', 0)),
            "booked": int(ghl.get('booked_appointments', 0)),
            "showed": int(ghl.get('showed_appointments', 0)),
            "closed": int(ghl.get('closed_deals', 0)),
        },
        "expense": {
            "labels": [c[0] for c in categories],
            "values": [c[1] for c in categories],
        },
        "ads": {
            "active": int(google_ads.get('active_products', 0)),
            "disapproved": int(google_ads.get('disapproved_products', 0)),
            "spend": ga_spend,
            "roas": ga_roas,
        },
        "trends": trend_data,
    }

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dr. Ruth Roberts - Weekly Dashboard | {week_ending_date}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa; color: #333; line-height: 1.5;
        }}
        .header {{
            background: linear-gradient(135deg, #1b5e20 0%, #2e7d32 100%);
            color: white; padding: 20px 30px; text-align: center;
        }}
        .header h1 {{ font-size: 24px; margin-bottom: 4px; }}
        .header .subtitle {{ opacity: 0.85; font-size: 14px; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        .alert-banner {{
            background: #fff3e0; border-left: 4px solid #e65100;
            padding: 15px 20px; margin-bottom: 20px; border-radius: 4px;
        }}
        .alert-banner h3 {{ color: #e65100; margin-bottom: 8px; }}
        .alert-banner ul {{ margin-left: 20px; color: #bf360c; }}
        .tier {{
            background: white; border-radius: 8px; margin-bottom: 16px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .tier-header {{
            padding: 16px 20px; cursor: pointer; display: flex;
            justify-content: space-between; align-items: center;
            border-bottom: 1px solid #eee; user-select: none;
        }}
        .tier-header:hover {{ background: #fafafa; }}
        .tier-header h2 {{ font-size: 16px; color: #1b5e20; }}
        .tier-toggle {{ font-size: 12px; color: #888; transition: transform 0.2s; }}
        .tier-body {{ padding: 20px; }}
        .tier-body.collapsed {{ display: none; }}
        .card-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
            gap: 12px; margin-bottom: 16px;
        }}
        .metric-card {{
            background: #fafbfc; border: 1px solid #e8ecef;
            border-radius: 6px; padding: 14px; text-align: center;
        }}
        .card-label {{ font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }}
        .card-value {{ font-size: 22px; font-weight: 700; color: #1b5e20; }}
        .card-wow {{ margin-top: 4px; }}
        .card-subtitle {{ margin-top: 4px; font-size: 11px; color: #999; }}
        .badge {{
            display: inline-block; padding: 2px 8px; border-radius: 10px;
            font-size: 11px; font-weight: 600;
        }}
        .badge-green {{ background: #e8f5e9; color: #2e7d32; }}
        .badge-red {{ background: #ffebee; color: #c62828; }}
        .badge-neutral {{ background: #f5f5f5; color: #666; }}
        .chart-container {{ margin: 16px 0; }}
        .data-table {{ width: 100%; border-collapse: collapse; margin: 12px 0; }}
        .data-table th {{ background: #f5f5f5; padding: 8px 12px; text-align: left; font-size: 12px; border-bottom: 2px solid #ddd; }}
        .data-table td {{ padding: 8px 12px; border-bottom: 1px solid #eee; font-size: 13px; }}
        .text-right {{ text-align: right; }}
        h3 {{ font-size: 14px; color: #444; margin: 16px 0 8px; }}
        h4 {{ font-size: 13px; color: #555; margin: 14px 0 6px; }}
        .footer {{ text-align: center; padding: 20px; color: #999; font-size: 12px; }}
        .chart-row {{
            display: flex; gap: 16px; margin-bottom: 20px;
        }}
        .chart-half {{
            flex: 1; min-width: 0;
            background: #fafbfc; border: 1px solid #e8ecef; border-radius: 6px;
            padding: 12px;
        }}
        .progress-row {{ margin: 8px 0; }}
        .progress-label {{ font-size: 12px; color: #555; margin-bottom: 4px; }}
        .progress-track {{
            background: #e8ecef; border-radius: 8px; height: 20px;
            overflow: hidden; position: relative;
        }}
        .progress-fill {{
            height: 100%; border-radius: 8px;
            transition: width 0.5s ease;
        }}
        @media (max-width: 768px) {{
            .card-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .card-value {{ font-size: 18px; }}
            .chart-row {{ flex-direction: column; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Dr. Ruth Roberts - Weekly Dashboard</h1>
        <div class="subtitle">Week ending: {week_ending_date} | Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</div>
    </div>

    <div class="container">
        {tier1}
        {tier2}
        {tier3}
        {tier4}
        {tier5}
        {tier6}
        {tier7}
        {tier8}
        {tier9}
        {tier10}
    </div>

    <div class="footer">
        DRR Dashboard v2.0 | Data refreshed weekly Monday 6:00 AM Central
    </div>

    <script>
    const chartData = {json.dumps(chart_data)};

    function toggleTier(id) {{
        const el = document.getElementById(id);
        if (el) el.classList.toggle('collapsed');
    }}

    // Revenue Doughnut Chart
    const revCtx = document.getElementById('revenueChart');
    if (revCtx) {{
        new Chart(revCtx, {{
            type: 'doughnut',
            data: {{
                labels: ['E-Commerce', 'Coaching', 'Courses'],
                datasets: [{{
                    data: [chartData.revenue.ecommerce, chartData.revenue.coaching, chartData.revenue.courses],
                    backgroundColor: ['#2e7d32', '#1565c0', '#e65100'],
                    borderWidth: 2,
                    borderColor: '#fff',
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ position: 'right' }},
                    title: {{ display: true, text: 'Revenue by Silo' }}
                }}
            }}
        }});
    }}

    // Expense Breakdown Pie Chart
    const expCtx = document.getElementById('expenseChart');
    if (expCtx) {{
        new Chart(expCtx, {{
            type: 'doughnut',
            data: {{
                labels: chartData.expense.labels,
                datasets: [{{
                    data: chartData.expense.values,
                    backgroundColor: [
                        '#2e7d32', '#43a047', '#1565c0', '#e65100',
                        '#6a1b9a', '#00838f', '#ef6c00', '#c62828'
                    ],
                    borderWidth: 2,
                    borderColor: '#fff',
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ position: 'right', labels: {{ font: {{ size: 11 }} }} }},
                    title: {{ display: true, text: 'Monthly Expense Breakdown' }}
                }}
            }}
        }});
    }}

    // Funnel Bar Chart
    const funnelCtx = document.getElementById('funnelChart');
    if (funnelCtx) {{
        new Chart(funnelCtx, {{
            type: 'bar',
            data: {{
                labels: ['New Leads', 'Booked', 'Showed', 'Closed'],
                datasets: [{{
                    label: 'Count',
                    data: [chartData.funnel.leads, chartData.funnel.booked, chartData.funnel.showed, chartData.funnel.closed],
                    backgroundColor: ['#c8e6c9', '#81c784', '#4caf50', '#2e7d32'],
                    borderRadius: 4,
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{ display: false }},
                    title: {{ display: true, text: 'Sales Funnel' }}
                }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});
    }}

    // Google Ads: Spend vs ROAS dual-axis (will populate with trend data)
    const adsSpendCtx = document.getElementById('adsSpendROAS');
    if (adsSpendCtx && chartData.ads.spend > 0) {{
        new Chart(adsSpendCtx, {{
            type: 'bar',
            data: {{
                labels: ['This Week'],
                datasets: [
                    {{
                        label: 'Spend',
                        data: [chartData.ads.spend],
                        backgroundColor: '#1565c0',
                        borderRadius: 4,
                        yAxisID: 'y',
                    }},
                    {{
                        label: 'Return',
                        data: [chartData.ads.spend * chartData.ads.roas],
                        backgroundColor: '#2e7d32',
                        borderRadius: 4,
                        yAxisID: 'y',
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    title: {{ display: true, text: 'Ad Spend vs Return' }},
                    legend: {{ position: 'bottom' }}
                }},
                scales: {{
                    y: {{ beginAtZero: true, ticks: {{ callback: v => '$' + v.toLocaleString() }} }}
                }}
            }}
        }});
    }}

    // Google Ads: Product Health Doughnut
    const adsHealthCtx = document.getElementById('adsProductHealth');
    if (adsHealthCtx && (chartData.ads.active > 0 || chartData.ads.disapproved > 0)) {{
        new Chart(adsHealthCtx, {{
            type: 'doughnut',
            data: {{
                labels: ['Active', 'Disapproved'],
                datasets: [{{
                    data: [chartData.ads.active, chartData.ads.disapproved],
                    backgroundColor: ['#2e7d32', '#c62828'],
                    borderWidth: 2,
                    borderColor: '#fff',
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    title: {{ display: true, text: 'Shopping Product Status' }},
                    legend: {{ position: 'bottom' }}
                }}
            }}
        }});
    }}

    // ── Trend Line Charts ──────────────────────────────────────────────
    const T = chartData.trends || {{}};
    if (T.labels && T.labels.length > 1) {{

        const lineDefaults = {{
            type: 'line',
            options: {{
                responsive: true,
                interaction: {{ mode: 'index', intersect: false }},
                plugins: {{ legend: {{ position: 'bottom', labels: {{ boxWidth: 12, padding: 8, font: {{ size: 11 }} }} }} }},
                scales: {{
                    x: {{ grid: {{ display: false }} }},
                    y: {{ beginAtZero: true }}
                }},
                elements: {{ point: {{ radius: 3, hoverRadius: 5 }}, line: {{ tension: 0.3 }} }}
            }}
        }};

        function dollarAxis(axis) {{
            return {{ ...axis, ticks: {{ callback: v => '$' + v.toLocaleString() }} }};
        }}
        function pctAxis(axis) {{
            return {{ ...axis, ticks: {{ callback: v => v + '%' }} }};
        }}

        // 1 ── Total Revenue
        new Chart(document.getElementById('trendRevenue'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [{{
                    label: 'Total Revenue',
                    data: T.total_revenue,
                    borderColor: '#2e7d32', backgroundColor: 'rgba(46,125,50,.1)',
                    fill: true, borderWidth: 2,
                }}]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Weekly Total Revenue' }} }},
                scales: {{ x: lineDefaults.options.scales.x, y: dollarAxis(lineDefaults.options.scales.y) }}
            }}
        }});

        // 2 ── Revenue by Silo
        new Chart(document.getElementById('trendRevBySilo'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [
                    {{ label: 'E-Commerce', data: T.ecommerce_revenue, borderColor: '#2e7d32', fill: false, borderWidth: 2 }},
                    {{ label: 'Coaching', data: T.coaching_revenue, borderColor: '#1565c0', fill: false, borderWidth: 2 }},
                    {{ label: 'Courses', data: T.course_revenue, borderColor: '#e65100', fill: false, borderWidth: 2 }},
                ]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Revenue by Silo' }} }},
                scales: {{ x: lineDefaults.options.scales.x, y: dollarAxis(lineDefaults.options.scales.y) }}
            }}
        }});

        // 3 ── NOI Trend
        if (document.getElementById('trendNOI')) {{
            new Chart(document.getElementById('trendNOI'), {{
                ...lineDefaults,
                data: {{
                    labels: T.labels,
                    datasets: [
                        {{ label: 'Revenue', data: T.total_revenue, borderColor: '#2e7d32', fill: false, borderWidth: 2 }},
                        {{ label: 'Burn', data: T.weekly_burn, borderColor: '#c62828', borderDash: [5,5], fill: false, borderWidth: 2 }},
                        {{ label: 'NOI', data: T.noi_weekly, borderColor: '#1565c0', backgroundColor: 'rgba(21,101,192,.1)', fill: true, borderWidth: 2 }},
                    ]
                }},
                options: {{ ...lineDefaults.options,
                    plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Revenue vs Burn vs NOI' }} }},
                    scales: {{ x: lineDefaults.options.scales.x, y: dollarAxis(lineDefaults.options.scales.y) }}
                }}
            }});
        }}

        // 4 ── Payroll % Trend
        if (document.getElementById('trendPayroll')) {{
            new Chart(document.getElementById('trendPayroll'), {{
                ...lineDefaults,
                data: {{
                    labels: T.labels,
                    datasets: [{{
                        label: 'Payroll %',
                        data: T.payroll_pct,
                        borderColor: '#6a1b9a', backgroundColor: 'rgba(106,27,154,.1)',
                        fill: true, borderWidth: 2,
                    }}]
                }},
                options: {{ ...lineDefaults.options,
                    plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Payroll as % of Revenue' }} }},
                    scales: {{
                        x: lineDefaults.options.scales.x,
                        y: {{ ...pctAxis(lineDefaults.options.scales.y),
                            suggestedMax: 40
                        }}
                    }}
                }}
            }});
        }}

        // 5 ── Ad Spend Trend
        if (document.getElementById('trendAdSpend') && T.ad_spend) {{
            new Chart(document.getElementById('trendAdSpend'), {{
                ...lineDefaults,
                data: {{
                    labels: T.labels,
                    datasets: [{{
                        label: 'Ad Spend',
                        data: T.ad_spend,
                        borderColor: '#1565c0', backgroundColor: 'rgba(21,101,192,.1)',
                        fill: true, borderWidth: 2,
                    }}]
                }},
                options: {{ ...lineDefaults.options,
                    plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Weekly Ad Spend' }} }},
                    scales: {{ x: lineDefaults.options.scales.x, y: dollarAxis(lineDefaults.options.scales.y) }}
                }}
            }});
        }}

        // 6 ── Ad ROAS Trend
        if (document.getElementById('trendAdROAS') && T.ad_roas) {{
            new Chart(document.getElementById('trendAdROAS'), {{
                ...lineDefaults,
                data: {{
                    labels: T.labels,
                    datasets: [{{
                        label: 'ROAS',
                        data: T.ad_roas,
                        borderColor: '#2e7d32', backgroundColor: 'rgba(46,125,50,.1)',
                        fill: true, borderWidth: 2,
                    }}]
                }},
                options: {{ ...lineDefaults.options,
                    plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Google Ads ROAS' }} }},
                    scales: {{
                        x: lineDefaults.options.scales.x,
                        y: {{ ...lineDefaults.options.scales.y, ticks: {{ callback: v => v + 'x' }}, suggestedMin: 0 }}
                    }}
                }}
            }});
        }}

        // 7 ── Email Revenue
        new Chart(document.getElementById('trendEmailRev'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [{{
                    label: 'Email Revenue',
                    data: T.email_revenue,
                    borderColor: '#6a1b9a', backgroundColor: 'rgba(106,27,154,.1)',
                    fill: true, borderWidth: 2,
                }}]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Klaviyo Email Revenue' }} }},
                scales: {{ x: lineDefaults.options.scales.x, y: dollarAxis(lineDefaults.options.scales.y) }}
            }}
        }});

        // 8 ── Email Open & Click Rates
        new Chart(document.getElementById('trendEmailRates'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [
                    {{ label: 'Open Rate %', data: T.open_rate, borderColor: '#2e7d32', fill: false, borderWidth: 2, yAxisID: 'y' }},
                    {{ label: 'Click Rate %', data: T.click_rate, borderColor: '#e65100', fill: false, borderWidth: 2, yAxisID: 'y1' }},
                ]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Email Open & Click Rates' }} }},
                scales: {{
                    x: lineDefaults.options.scales.x,
                    y: {{ ...pctAxis(lineDefaults.options.scales.y), position: 'left', title: {{ display: true, text: 'Open Rate' }} }},
                    y1: {{ ...pctAxis(lineDefaults.options.scales.y), position: 'right', grid: {{ drawOnChartArea: false }}, title: {{ display: true, text: 'Click Rate' }} }},
                }}
            }}
        }});

        // 9 ── Orders
        new Chart(document.getElementById('trendOrders'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [
                    {{ label: 'Orders', data: T.order_count, borderColor: '#2e7d32', backgroundColor: 'rgba(46,125,50,.1)', fill: true, borderWidth: 2 }},
                    {{ label: 'New Customers', data: T.new_customers, borderColor: '#1565c0', fill: false, borderWidth: 2 }},
                ]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Orders & New Customers' }} }}
            }}
        }});

        // 10 ── AOV
        new Chart(document.getElementById('trendAOV'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [{{
                    label: 'AOV',
                    data: T.aov,
                    borderColor: '#e65100', backgroundColor: 'rgba(230,81,0,.1)',
                    fill: true, borderWidth: 2,
                }}]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Average Order Value' }} }},
                scales: {{ x: lineDefaults.options.scales.x, y: dollarAxis(lineDefaults.options.scales.y) }}
            }}
        }});

        // 11 ── MRR
        new Chart(document.getElementById('trendMRR'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [{{
                    label: 'MRR',
                    data: T.mrr,
                    borderColor: '#1565c0', backgroundColor: 'rgba(21,101,192,.1)',
                    fill: true, borderWidth: 2,
                }}]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Monthly Recurring Revenue (MRR)' }} }},
                scales: {{ x: lineDefaults.options.scales.x, y: dollarAxis(lineDefaults.options.scales.y) }}
            }}
        }});

        // 12 ── Stripe Volume
        new Chart(document.getElementById('trendStripeVol'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [{{
                    label: 'Gross Volume',
                    data: T.gross_volume,
                    borderColor: '#2e7d32', backgroundColor: 'rgba(46,125,50,.1)',
                    fill: true, borderWidth: 2,
                }}]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Stripe Gross Volume' }} }},
                scales: {{ x: lineDefaults.options.scales.x, y: dollarAxis(lineDefaults.options.scales.y) }}
            }}
        }});

        // 13 ── Social: FB + IG
        new Chart(document.getElementById('trendSocial'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [
                    {{ label: 'FB Followers', data: T.fb_followers, borderColor: '#1565c0', fill: false, borderWidth: 2 }},
                    {{ label: 'IG Followers', data: T.ig_followers, borderColor: '#e65100', fill: false, borderWidth: 2 }},
                ]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Facebook & Instagram Followers' }} }}
            }}
        }});

        // 14 ── YouTube
        new Chart(document.getElementById('trendYT'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [{{
                    label: 'YT Subscribers',
                    data: T.yt_subscribers,
                    borderColor: '#c62828', backgroundColor: 'rgba(198,40,40,.1)',
                    fill: true, borderWidth: 2,
                }}]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'YouTube Subscribers' }} }}
            }}
        }});

        // 15 ── Pipeline
        new Chart(document.getElementById('trendPipeline'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [
                    {{ label: 'New Leads', data: T.new_leads, borderColor: '#2e7d32', fill: false, borderWidth: 2 }},
                    {{ label: 'Booked Calls', data: T.booked, borderColor: '#1565c0', fill: false, borderWidth: 2 }},
                ]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Sales Pipeline Activity' }} }}
            }}
        }});

        // 16 ── Active Students
        new Chart(document.getElementById('trendStudents'), {{
            ...lineDefaults,
            data: {{
                labels: T.labels,
                datasets: [{{
                    label: 'Active Students',
                    data: T.active_students,
                    borderColor: '#6a1b9a', backgroundColor: 'rgba(106,27,154,.1)',
                    fill: true, borderWidth: 2,
                }}]
            }},
            options: {{ ...lineDefaults.options,
                plugins: {{ ...lineDefaults.options.plugins, title: {{ display: true, text: 'Active Coaching Students' }} }}
            }}
        }});

    }} // end if trends
    </script>
</body>
</html>"""

    return html


def save_dashboard(html, output_path='dashboard/index.html'):
    """Save the static dashboard HTML to dashboard/ for backup viewing.

    The repo root index.html is the DYNAMIC dashboard (loads from
    snapshots/manifest.json) and is NEVER overwritten by this function.
    GitHub Pages serves the dynamic version from root, which auto-updates
    whenever a new snapshot is added to manifest.json.
    """
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"Static dashboard saved to {output_path}")
    logger.info("Root index.html is dynamic — not overwritten")
