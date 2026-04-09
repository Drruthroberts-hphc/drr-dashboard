"""
Configuration for DRR Dashboard
================================
KPI definitions, silo mapping, alert thresholds, and shared constants.
"""

import os
import re
from dotenv import load_dotenv

load_dotenv()

# ── API Configuration ──────────────────────────────────────────────────────

SHOPIFY_STORE = os.getenv('SHOPIFY_STORE', 'drruthroberts-com.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN', '')
SHOPIFY_API_VERSION = '2026-01'
SHOPIFY_BASE_URL = f"https://{SHOPIFY_STORE}/admin/api/{SHOPIFY_API_VERSION}"

KLAVIYO_API_KEY = os.getenv('KLAVIYO_API_KEY', '')
STRIPE_API_KEY = os.getenv('STRIPE_API_KEY', '')

GHL_API_KEY = os.getenv('GHL_API_KEY', '')
GHL_LOCATION_ID = os.getenv('GHL_LOCATION_ID', '')
GHL_BASE_URL = 'https://services.leadconnectorhq.com'

YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY', '')
YOUTUBE_CHANNEL_ID = os.getenv('YOUTUBE_CHANNEL_ID', '')

META_PAGE_ACCESS_TOKEN = os.getenv('META_PAGE_ACCESS_TOKEN', '')
FB_PAGE_ID = os.getenv('FB_PAGE_ID', '')
IG_ACCOUNT_ID = os.getenv('IG_ACCOUNT_ID', '')

GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID', '14apn1B2poeYVxf-XFgtkoVgdyqp3UlAR2hMuJYDF_CQ')
GOOGLE_SHEETS_CREDENTIALS = os.getenv('GOOGLE_SHEETS_CREDENTIALS', 'credentials.json')

GMAIL_CREDENTIALS = os.getenv('GMAIL_CREDENTIALS', 'gmail_credentials.json')
GMAIL_TOKEN = os.getenv('GMAIL_TOKEN', 'gmail_token.json')

# Alert / weekly-summary recipients
ALERT_EMAILS = [
    'dr.ruth.roberts4pets@gmail.com',
    'carol@minepetplatter.com',
    'jenny@drruthroberts.com',
]

# Rana's identifier for GHL filtering
RANA_EMAIL = 'rana@holisticpethealthcoach.com'

# ── Revenue Silo Classification ───────────────────────────────────────────

# Coaching vendors (exact match)
COACHING_VENDORS = {'CHPHC Consultation', 'Coaches-Dr. Ruth Roberts'}

# Coaching product types (exact match)
COACHING_TYPES = {'Coaching', 'Health Coaching Package'}

# Course vendors (exact match)
COURSE_VENDORS = {"Dr. Ruth's Courses"}

# Course product types (exact match)
COURSE_TYPES = {'DIY Holistic Health Course'}

# CrockPET ebook/recipe keywords (title must contain one of these AND crockpet)
CROCKPET_COURSE_KEYWORDS = {'ebook', 'recipe', 'e-book', 'download'}

# Certification keywords
CERTIFICATION_KEYWORDS = {'certification program'}


def classify_product_silo(title, vendor, product_type):
    """
    Classify a Shopify product into a revenue silo.

    Priority rules (checked in order):
    1. Vendor is a coaching vendor → Coaching
    2. Type is a coaching type → Coaching
    3. Vendor is a course vendor OR type is a course type → Courses
    4. Title contains 'CrockPET/Crockpet' AND ebook/recipe keyword → Courses
    5. Title contains 'Certification Program' → Coaching
    6. Everything else → E-Commerce

    Note: Physical CrockPET diet kits (starter kit, refill kit) are E-Commerce.
    Only ebooks/recipes/downloads are classified as Courses.
    """
    title_lower = (title or '').lower().strip()
    vendor_clean = (vendor or '').strip()
    type_clean = (product_type or '').strip()

    # Rule 1: Coaching vendors
    if vendor_clean in COACHING_VENDORS:
        return 'Coaching'

    # Rule 2: Coaching types
    if type_clean in COACHING_TYPES:
        return 'Coaching'

    # Rule 3: Course vendors or types
    if vendor_clean in COURSE_VENDORS or type_clean in COURSE_TYPES:
        return 'Courses'

    # Rule 4: CrockPET ebook/recipe only (NOT physical kits)
    if 'crockpet' in title_lower or 'crockpet' in title_lower.replace(' ', ''):
        if any(kw in title_lower for kw in CROCKPET_COURSE_KEYWORDS):
            return 'Courses'

    # Rule 5: Certification programs
    if any(kw in title_lower for kw in CERTIFICATION_KEYWORDS):
        return 'Coaching'

    # Rule 6: Default
    return 'E-Commerce'


# ── Google Ads Configuration ──────────────────────────────────────────────

# Google Ads API credentials
GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN', '')
GOOGLE_ADS_CLIENT_ID = os.getenv('GOOGLE_ADS_CLIENT_ID', '')
GOOGLE_ADS_CLIENT_SECRET = os.getenv('GOOGLE_ADS_CLIENT_SECRET', '')
GOOGLE_ADS_REFRESH_TOKEN = os.getenv('GOOGLE_ADS_REFRESH_TOKEN', '')
GOOGLE_ADS_CUSTOMER_ID = os.getenv('GOOGLE_ADS_CUSTOMER_ID', '')
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID', '')

# ── Financial Constants (verified from bank statements March 2026) ────────
# Update these when costs change. All values monthly.

FINANCIAL = {
    # ── Team Costs ────────────────────────────────────────────────────────
    'team': {
        'ruth_salary': 7000,       # Owner salary target
        'jenny_pm': 1500,          # Project manager / systems / SOPs
        'danty_seo': 900,          # SEO + email marketing (replacing Perfect Labs)
        'erica_admin': 900,        # Admin support
        'joy_dev': 900,            # Web development
        'beth_cs': 1000,           # Customer service (coaches)
        'jenn_cs': 531,            # Customer service (K9 Fitness Works)
        'max_video': 500,          # Video editing
        'dlorah_special': 200,     # Special projects
        'rana_commission_pct': 0.10,  # 10% of coaching sales (variable)
    },
    'team_fixed_total': 13431,     # All team except Rana commission ($14,431 incl $1k avg Rana)

    # ── Variable Costs ────────────────────────────────────────────────────
    'google_ads_budget': 2500,     # Monthly Google Ads budget
    'cogs_standard_process': 5000, # Standard Process COGS
    'cogs_other': 2000,            # Other COGS / shipping materials
    'mentor_cost_per_student': 800,  # Paid per student per cohort
    'mentor_monthly_est': 3200,    # ~4 students avg
    'healthy_life_shipping': 1750, # Pick & pack fulfillment
    'stripe_processing_est': 2500, # ~3.5% of Stripe volume
    'shopify_fees_est': 500,       # Shopify transaction fees
    'paypal_fees_est': 200,        # PayPal fees
    'fullscript_referral_est': 157,  # Fullscript referral commissions

    # ── Fixed Services ────────────────────────────────────────────────────
    'ghl_total': 1028,             # $531 main + $497 Agency Pro
    'klaviyo': 670,                # Email platform
    'shopify_plan': 399,           # Shopify Plus
    'circle_so': 99,               # Course platform
    'tickner_monthly': 1250,       # Scale Systems ($15k/yr)
    'harquin_accounting': 400,     # Accounting + QuickBooks
    'perfect_labs': 1000,          # SEO (cutting in ~2 months)
    'delphi_ai': 350,              # AI clone (under review)
    'mexico_dev': 1200,            # Business in a Box platforms
    'revolut_fee': 50,             # Banking fee
    'other_software': 200,         # Misc SaaS (annual amortized: Opus Clip, Consensus, Zoom, etc.)

    # ── Debt ──────────────────────────────────────────────────────────────
    'total_debt': 219000,          # As of March 2026
    'debt_breakdown': {
        'existing': 169000,        # Pre-existing obligations
        'carol_branding': 30000,   # Our Pet Project (remaining)
        'event_sponsorship': 20000,  # Event sponsorship
    },
    'monthly_debt_service': 5000,  # Estimated monthly payments

    # ── Revenue Targets ───────────────────────────────────────────────────
    'revenue_target_monthly': 65000,  # Current realistic target
    'revenue_targets_by_month': {
        '2026-01': 85000,
        '2026-02': 65000,
        '2026-03': 65000,
        '2026-04': 65000,
        '2026-05': 70000,
        '2026-06': 75000,
    },

    # ── Calculated ────────────────────────────────────────────────────────
    'total_monthly_burn': 42486,
}

# Convenience calculations
FINANCIAL['services_total'] = (
    FINANCIAL['ghl_total'] + FINANCIAL['klaviyo'] + FINANCIAL['shopify_plan'] +
    FINANCIAL['circle_so'] + FINANCIAL['tickner_monthly'] + FINANCIAL['harquin_accounting'] +
    FINANCIAL['perfect_labs'] + FINANCIAL['delphi_ai'] + FINANCIAL['mexico_dev'] +
    FINANCIAL['revolut_fee'] + FINANCIAL['other_software']
)

# ── Dashboard Tier Structure ──────────────────────────────────────────────

TIERS = {
    1: {'name': 'Executive Overview', 'description': 'Revenue, burn rate, NOI, debt paydown, targets vs actuals'},
    2: {'name': 'Financial Health', 'description': 'Payroll %, cash flow, debt tracker, expense breakdown'},
    3: {'name': 'Google Ads', 'description': 'Spend, ROAS, CPA, conversions, product disapprovals'},
    4: {'name': 'Sales Pipeline (GHL)', 'description': 'Leads, appointments, close rates, Rana metrics'},
    5: {'name': 'Email Marketing (Klaviyo)', 'description': 'Open/click rates, flow revenue, list health'},
    6: {'name': 'E-Commerce Detail (Shopify)', 'description': 'Conversion, AOV, customers, top products'},
    7: {'name': 'Payments & Health (Stripe)', 'description': 'Success rate, disputes, refunds, MRR'},
    8: {'name': 'Coaching Program', 'description': 'Enrollment, churn, revenue per student'},
    9: {'name': 'Social Media', 'description': 'YouTube, Facebook, Instagram metrics'},
}

# ── Alert Thresholds ──────────────────────────────────────────────────────

ALERT_THRESHOLDS = [
    {
        'metric': 'spam_complaint_rate',
        'display_name': 'Spam Complaint Rate',
        'threshold': 0.001,  # 0.1%
        'direction': 'above',
        'platform': 'Klaviyo',
    },
    {
        'metric': 'dispute_rate',
        'display_name': 'Dispute/Chargeback Rate',
        'threshold': 0.005,  # 0.5%
        'direction': 'above',
        'platform': 'Stripe',
    },
    {
        'metric': 'payment_success_rate',
        'display_name': 'Payment Success Rate',
        'threshold': 0.95,  # 95%
        'direction': 'below',
        'platform': 'Stripe',
    },
    {
        'metric': 'revenue_wow_change',
        'display_name': 'Revenue WoW Change',
        'threshold': -0.25,  # -25%
        'direction': 'below',
        'platform': 'Cross-platform',
    },
    {
        'metric': 'close_rate',
        'display_name': 'Close Rate',
        'threshold': 0.30,  # 30%
        'direction': 'below',
        'platform': 'GHL',
    },
    {
        'metric': 'email_delivery_rate',
        'display_name': 'Email Delivery Rate',
        'threshold': 0.95,  # 95%
        'direction': 'below',
        'platform': 'Klaviyo',
    },
    {
        'metric': 'shopify_conversion_rate',
        'display_name': 'Shopify Conversion Rate',
        'threshold': 0.015,  # 1.5%
        'direction': 'below',
        'platform': 'Shopify',
    },
    {
        'metric': 'student_churn_rate',
        'display_name': 'Student Churn Rate',
        'threshold': 0.15,  # 15%
        'direction': 'above',
        'platform': 'TBD',
    },
    {
        'metric': 'google_ads_roas',
        'display_name': 'Google Ads ROAS',
        'threshold': 2.5,
        'direction': 'below',
        'platform': 'Google Ads',
    },
    {
        'metric': 'google_ads_weekly_spend',
        'display_name': 'Google Ads Weekly Spend',
        'threshold': 750,  # $2,500/mo ÷ 4 = $625/wk, alert at $750 (20% over)
        'direction': 'above',
        'platform': 'Google Ads',
    },
    {
        'metric': 'payroll_pct_of_revenue',
        'display_name': 'Payroll % of Revenue',
        'threshold': 0.30,  # 30% of revenue
        'direction': 'above',
        'platform': 'Financial',
    },
    {
        'metric': 'burn_rate_exceeds_revenue',
        'display_name': 'Burn > Revenue (Weekly)',
        'threshold': 1.0,  # burn/revenue ratio > 1 = losing money
        'direction': 'above',
        'platform': 'Financial',
    },
]

# ── Google Sheets Tab Structure ───────────────────────────────────────────

SHEET_TABS = {
    'Klaviyo_Weekly': [
        'week_ending_date', 'email_attributed_revenue', 'welcome_flow_revenue',
        'abandon_cart_flow_revenue', 'post_purchase_flow_revenue', 'open_rate',
        'click_rate', 'ctor', 'list_size', 'list_growth_rate',
        'spam_complaint_rate', 'delivery_rate', 'abandon_cart_recovery_rate',
        'abandon_cart_recovery_revenue',
    ],
    'Shopify_Weekly': [
        'week_ending_date', 'gross_revenue', 'net_revenue', 'ecommerce_revenue',
        'coaching_revenue', 'course_revenue', 'order_count', 'aov',
        'conversion_rate', 'new_customers', 'returning_customers',
        'cart_abandonment_rate', 'checkout_abandonment_rate', 'return_rate',
        'discount_rate_pct', 'top_products_json',
    ],
    'GHL_Weekly': [
        'week_ending_date', 'new_leads', 'booked_appointments',
        'showed_appointments', 'closed_deals', 'close_rate_overall',
        'close_rate_rana', 'pipeline_value', 'revenue_per_call',
        'active_students', 'enrollment_growth_rate', 'student_churn_rate',
        'revenue_per_student',
    ],
    'Stripe_Weekly': [
        'week_ending_date', 'gross_payment_volume', 'net_revenue',
        'payment_success_rate', 'dispute_rate', 'refund_rate', 'refund_amount',
        'mrr', 'processing_fees',
    ],
    'CrossPlatform_Weekly': [
        'week_ending_date', 'total_revenue', 'coaching_revenue',
        'ecommerce_revenue', 'course_revenue', 'burn_rate', 'net_profit_loss',
        'total_email_revenue', 'total_close_rate',
    ],
    'Social_Weekly': [
        'week_ending_date', 'yt_subscribers', 'yt_sub_growth', 'yt_views',
        'yt_watch_hours', 'yt_new_videos', 'yt_comments', 'fb_followers',
        'fb_follower_growth', 'fb_reach', 'fb_engagement_rate', 'fb_messages',
        'ig_followers', 'ig_follower_growth', 'ig_engagement_rate',
        'ig_story_views', 'ig_dms',
    ],
    'GoogleAds_Weekly': [
        'week_ending_date', 'ad_spend', 'conversions', 'conversion_value',
        'roas', 'cpa', 'clicks', 'impressions', 'ctr',
        'active_products', 'disapproved_products', 'disapproval_rate',
    ],
    'Financial_Weekly': [
        'week_ending_date', 'weekly_revenue', 'monthly_revenue_run_rate',
        'weekly_burn', 'monthly_burn', 'noi', 'noi_margin',
        'payroll_pct', 'debt_remaining', 'months_to_debt_free',
        'cash_flow_weekly', 'revenue_vs_target_pct',
    ],
    'Alerts_Log': [
        'timestamp', 'metric_name', 'current_value', 'threshold',
        'direction', 'status', 'notified',
    ],
}
