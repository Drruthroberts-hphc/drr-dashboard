"""
Dashboard HTML Generator
========================
Generates a single-file interactive HTML dashboard with Chart.js charts,
7 collapsible tiers, and 54 KPIs. Output is suitable for GitHub Pages.
"""

import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def _fmt_money(val):
    """Format as currency."""
    try:
        v = float(val)
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


def _metric_card(label, value, wow_html="", subtitle=""):
    """Generate a metric card HTML snippet."""
    sub = f'<div class="card-subtitle">{subtitle}</div>' if subtitle else ""
    wow = f'<div class="card-wow">{wow_html}</div>' if wow_html else ""
    return f"""
    <div class="metric-card">
        <div class="card-label">{label}</div>
        <div class="card-value">{value}</div>
        {wow}
        {sub}
    </div>"""


def generate_dashboard(all_data, cross_data, alerts, previous_data=None, week_ending_date=None):
    """
    Generate the full single-file HTML dashboard.

    Args:
        all_data: dict with 'shopify', 'klaviyo', 'stripe', 'ghl', 'social' data
        cross_data: dict with cross-platform metrics
        alerts: list of triggered alerts
        previous_data: dict with previous week's data (same structure as all_data)
        week_ending_date: string date for the week

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

    prev = previous_data or {}
    prev_shopify = prev.get('shopify') or {}
    prev_klaviyo = prev.get('klaviyo') or {}
    prev_stripe = prev.get('stripe') or {}
    prev_ghl = prev.get('ghl') or {}
    prev_social = prev.get('social') or {}
    prev_cross = prev.get('cross_platform') or {}

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

    # ── Tier 1: Executive Overview ────────────────────────────────────────
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
                {_metric_card("Email Revenue", _fmt_money(cross_data.get('total_email_revenue', 0)))}
                {_metric_card("MRR", _fmt_money(stripe_data.get('mrr', 0)))}
                {_metric_card("Net Profit/Loss", _fmt_money(cross_data.get('net_profit_loss', 0)))}
                {_metric_card("Active Students", _fmt_int(ghl.get('active_students', 0)))}
            </div>
            <div class="chart-container">
                <canvas id="revenueChart" height="250"></canvas>
            </div>
        </div>
    </div>"""

    # ── Tier 2: Sales Pipeline (GHL) ──────────────────────────────────────
    tier2 = f"""
    <div class="tier" id="tier2">
        <div class="tier-header" onclick="toggleTier('tier2-body')">
            <h2>&#9656; Tier 2: Sales Pipeline (GHL)</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier2-body">
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

    # ── Tier 3: Email Marketing (Klaviyo) ─────────────────────────────────
    tier3 = f"""
    <div class="tier" id="tier3">
        <div class="tier-header" onclick="toggleTier('tier3-body')">
            <h2>&#9656; Tier 3: Email Marketing (Klaviyo)</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier3-body">
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

    # ── Tier 4: E-Commerce Detail (Shopify) ───────────────────────────────
    top_products_html = ""
    try:
        top_products = json.loads(shopify.get('top_products_json', '[]'))
        if top_products:
            rows = "".join(
                f'<tr><td>{p.get("title","")[:40]}</td>'
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

    tier4 = f"""
    <div class="tier" id="tier4">
        <div class="tier-header" onclick="toggleTier('tier4-body')">
            <h2>&#9656; Tier 4: E-Commerce Detail (Shopify)</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier4-body">
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

    # ── Tier 5: Payments & Health (Stripe) ────────────────────────────────
    tier5 = f"""
    <div class="tier" id="tier5">
        <div class="tier-header" onclick="toggleTier('tier5-body')">
            <h2>&#9656; Tier 5: Payments &amp; Health (Stripe)</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier5-body">
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

    # ── Tier 6: Coaching Program ──────────────────────────────────────────
    tier6 = f"""
    <div class="tier" id="tier6">
        <div class="tier-header" onclick="toggleTier('tier6-body')">
            <h2>&#9656; Tier 6: Coaching Program</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier6-body">
            <div class="card-grid">
                {_metric_card("Active Students", _fmt_int(ghl.get('active_students', 0)),
                    _wow_badge(ghl.get('active_students',0), prev_ghl.get('active_students',0)))}
                {_metric_card("Enrollment Growth", _fmt_pct(ghl.get('enrollment_growth_rate', 0)))}
                {_metric_card("Student Churn", _fmt_pct(ghl.get('student_churn_rate', 0)),
                    subtitle="Target: < 15%")}
                {_metric_card("Rev per Student", _fmt_money(ghl.get('revenue_per_student', 0)))}
            </div>
        </div>
    </div>"""

    # ── Tier 7: Social Media ──────────────────────────────────────────────
    tier7 = f"""
    <div class="tier" id="tier7">
        <div class="tier-header" onclick="toggleTier('tier7-body')">
            <h2>&#9656; Tier 7: Social Media</h2>
            <span class="tier-toggle">&#9660;</span>
        </div>
        <div class="tier-body" id="tier7-body">
            <h3>YouTube</h3>
            <div class="card-grid">
                {_metric_card("Subscribers", _fmt_int(social.get('yt_subscribers', 0)),
                    _wow_badge(social.get('yt_subscribers',0), prev_social.get('yt_subscribers',0)))}
                {_metric_card("Views (This Week)", _fmt_int(social.get('yt_views', 0)))}
                {_metric_card("Watch Hours", _fmt_float(social.get('yt_watch_hours', 0)))}
                {_metric_card("New Videos", _fmt_int(social.get('yt_new_videos', 0)))}
                {_metric_card("Comments", _fmt_int(social.get('yt_comments', 0)))}
            </div>
            <h3>Facebook &amp; Instagram <span class="phase-badge">Phase 2</span></h3>
            <div class="card-grid">
                {_metric_card("FB Followers", _fmt_int(social.get('fb_followers', 0)))}
                {_metric_card("FB Reach", _fmt_int(social.get('fb_reach', 0)))}
                {_metric_card("IG Followers", _fmt_int(social.get('ig_followers', 0)))}
                {_metric_card("IG Engagement", _fmt_pct(social.get('ig_engagement_rate', 0)))}
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
        .phase-badge {{
            background: #e3f2fd; color: #1565c0; padding: 2px 8px;
            border-radius: 10px; font-size: 10px; font-weight: 600; vertical-align: middle;
        }}
        h3 {{ font-size: 14px; color: #444; margin: 16px 0 8px; }}
        .footer {{ text-align: center; padding: 20px; color: #999; font-size: 12px; }}
        @media (max-width: 768px) {{
            .card-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .card-value {{ font-size: 18px; }}
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
    </div>

    <div class="footer">
        DRR Dashboard v1.0 | Data refreshed weekly Monday 6:00 AM Central
    </div>

    <script>
    const chartData = {json.dumps(chart_data)};

    function toggleTier(id) {{
        const el = document.getElementById(id);
        if (el) el.classList.toggle('collapsed');
    }}

    // Revenue Pie Chart
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
    </script>
</body>
</html>"""

    return html


def save_dashboard(html, output_path='dashboard/index.html'):
    """Save the dashboard HTML to a file."""
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"Dashboard saved to {output_path}")
