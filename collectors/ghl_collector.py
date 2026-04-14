"""
GoHighLevel Data Collector
==========================
Collects sales pipeline metrics: leads, appointments, close rates,
Rana's metrics, and student success data from the GHL API.

Pipeline structure (discovered via API):
  Pipeline 1 "1 Sales Pipeline" (zOxFfLUa0n5lxOgSnZPN):
    - New Leads / AD Lead Form / Opted In (Exit Intent)
    - Discovery Call Booked
    - Booked Call No Show/Cancelled
    - Discovery Call Completed
    - Live Workshop Scheduled
    - Ready to invest / Not Ready to Invest
    - Maybe - Nurture / No response for more than 5 days / Dead Lead

  Pipeline 2 "2 Student Success" (Q443OfUgQXeEwqAMZ7kY):
    - Agreement Signed / Paid: Self Paced / Deposit Paid: 4mos / Deposit Paid: 12 mos
    - Assigned To [Jessica/Michael/Dlorah/Natalie/Deb]
    - Graduates / Past Mentoring Time / Not Eligible for Graduation / Renewed
"""

import json
import logging
import os
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict

from config import GHL_API_KEY, GHL_LOCATION_ID, GHL_BASE_URL, RANA_EMAIL

logger = logging.getLogger(__name__)

# Path to pre-fetched cache (populated via MCP tools when API key has JWT issues)
CACHE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ghl_cache.json')

# Pipeline IDs
SALES_PIPELINE_ID = 'zOxFfLUa0n5lxOgSnZPN'
STUDENT_PIPELINE_ID = 'Q443OfUgQXeEwqAMZ7kY'

# Stage IDs - Sales Pipeline
STAGE_NEW_LEADS = '2ed1a2ac-b183-46d8-9eda-50b32d89b714'
STAGE_AD_LEAD = '34339426-3a12-4f25-b5b1-b32588e37a9b'
STAGE_OPTED_IN = 'b30a3f41-432e-47b2-aaa8-f54f8ed36098'
STAGE_CALL_BOOKED = '48290f28-9f87-449a-983d-75a554b0b533'
STAGE_NO_SHOW = '24a800a7-74bc-41ab-b5c0-711025b3bdae'
STAGE_CALL_COMPLETED = '2f32706d-a486-4fdb-9fe8-d2e4a448cdda'
STAGE_WORKSHOP = '3e3c64c2-17ae-4547-9226-f6ca35c69489'
STAGE_READY_INVEST = '2a3293c1-1d1d-48f5-88d4-602c3eabb8c9'
STAGE_NOT_READY = '569475e1-9d5f-40a8-a504-e7bacbe3091a'
STAGE_NURTURE = '39da29fd-460d-41bd-967c-9dff388bf2a4'
STAGE_NO_RESPONSE = '3a993044-dd8b-4bbb-8cd2-4708cf7284d3'
STAGE_DEAD_LEAD = '8fd699b6-e073-4bac-823a-2eb6400a24fc'

# Stage IDs - Student Success Pipeline
STAGE_AGREEMENT_SIGNED = '62b0cfbe-e1ad-4eb8-b424-2042ebb62ad9'
STAGE_PAID_SELF_PACED = '5281c1b7-d2c7-46b9-ad24-41a88f68d478'
STAGE_DEPOSIT_4MO = '998863d1-e41e-40b9-bf48-ede55a6bc39e'
STAGE_DEPOSIT_12MO = '3c9ed578-192d-48c8-88f3-d5bf4b3a93be'
STAGE_ASSIGNED_JESSICA = '6d7cf9b6-b8c7-47ee-824b-d57fa9c0f5fb'
STAGE_ASSIGNED_MICHAEL = '74abd1e2-fbf8-4875-91e9-a81011f30fcd'
STAGE_ASSIGNED_DLORAH = '961bc10a-12be-45d2-a259-da35cc8e0c65'
STAGE_ASSIGNED_NATALIE = 'b4e1899e-1dad-4161-90c7-a16651c1a6a6'
STAGE_ASSIGNED_DEB = '3559d0ec-1ac3-46b2-b41c-b5a5e13e0ef9'
STAGE_GRADUATES = '6410aa83-5b5f-4128-a070-86d86361b9be'
STAGE_PAST_MENTORING = 'ce78abac-140c-44fa-8299-ce48e3dc708f'
STAGE_NOT_ELIGIBLE = '6fc3c86f-f51a-47eb-bace-a8e582960ccc'
STAGE_RENEWED = 'f0fbaf02-80e7-46fd-b589-3e9f1ae9a63b'

# "Lead" stages (top of funnel)
LEAD_STAGES = {STAGE_NEW_LEADS, STAGE_AD_LEAD, STAGE_OPTED_IN}

# "Booked" stage
BOOKED_STAGES = {STAGE_CALL_BOOKED}

# "Showed" stage (completed discovery call)
SHOWED_STAGES = {STAGE_CALL_COMPLETED}

# "Closed" stage (ready to invest = converted)
CLOSED_STAGES = {STAGE_READY_INVEST}

# Active student stages (enrolled and receiving coaching)
ACTIVE_STUDENT_STAGES = {
    STAGE_AGREEMENT_SIGNED, STAGE_PAID_SELF_PACED,
    STAGE_DEPOSIT_4MO, STAGE_DEPOSIT_12MO,
    STAGE_ASSIGNED_JESSICA, STAGE_ASSIGNED_MICHAEL,
    STAGE_ASSIGNED_DLORAH, STAGE_ASSIGNED_NATALIE,
    STAGE_ASSIGNED_DEB,
}

# Churned/completed stages
CHURNED_STAGES = {STAGE_PAST_MENTORING, STAGE_NOT_ELIGIBLE}
GRADUATED_STAGES = {STAGE_GRADUATES, STAGE_RENEWED}


V1_BASE_URL = 'https://rest.gohighlevel.com/v1'


def _ghl_v1_get_all_opportunities(pipeline_id, max_pages=None):
    """Fetch opportunities from a v1 pipeline, handling pagination.

    v1 returns 20 per page with nextPageUrl containing startAfter/startAfterId.

    Args:
        pipeline_id: GHL pipeline ID
        max_pages: Max pages to fetch (None = all). Use for large pipelines
                   like Sales (15K+ records) where we only need recent data.
    """
    import time
    all_opps = []
    url = f"{V1_BASE_URL}/pipelines/{pipeline_id}/opportunities"
    page_count = 0

    while url:
        page_count += 1

        # Rate-limit: pause between pages to avoid 429s
        if page_count > 1:
            time.sleep(0.5)

        req = urllib.request.Request(url, headers={
            'Authorization': f'Bearer {GHL_API_KEY}',
            'Accept': 'application/json',
            'User-Agent': 'DRR-Dashboard/1.0',
        })
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')[:200] if e.fp else 'no body'
            logger.error(f"GHL v1 pagination error {e.code} on page {page_count}: {body}")
            if e.code == 429:
                logger.warning("Rate limited — returning what we have so far")
            break
        except Exception as e:
            logger.error(f"GHL v1 request failed on page {page_count}: {e}")
            break

        opps = data.get('opportunities', [])
        all_opps.extend(opps)

        meta = data.get('meta', {})
        total = meta.get('total', len(all_opps))
        next_url = meta.get('nextPageUrl')

        # v1 returns http:// URLs — upgrade to https://
        if next_url:
            url = next_url.replace('http://', 'https://')
        else:
            url = None

        if not opps:
            break

        # Respect max_pages limit
        if max_pages and page_count >= max_pages:
            logger.info(f"Pipeline {pipeline_id}: stopped at {max_pages} pages "
                        f"({len(all_opps)} of {total} total)")
            break

    logger.info(f"Pipeline {pipeline_id}: fetched {len(all_opps)} opportunities "
                f"({page_count} pages)")
    return all_opps


def _ghl_v1_get(endpoint, params=None):
    """Make an authenticated GET request to the GHL v1 API."""
    url = f"{V1_BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {GHL_API_KEY}',
        'Accept': 'application/json',
        'User-Agent': 'DRR-Dashboard/1.0',
    })

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')[:300]
        logger.error(f"GHL v1 API error {e.code} on {endpoint}: {body}")
        return None


def _ghl_get(endpoint, params=None):
    """Make an authenticated GET request to the GHL API (v2 — may 401)."""
    url = f"{GHL_BASE_URL}/{endpoint}"
    if params:
        url += '?' + urllib.parse.urlencode(params)

    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {GHL_API_KEY}',
        'Version': '2021-07-28',
        'Accept': 'application/json',
        'User-Agent': 'DRR-Dashboard/1.0',
    })

    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')[:300]
        logger.error(f"GHL API error {e.code} on {endpoint}: {body}")
        return None


def _ghl_search_opportunities(pipeline_id, params=None):
    """Search opportunities in a pipeline."""
    base_params = {
        'location_id': GHL_LOCATION_ID,
        'pipeline_id': pipeline_id,
        'limit': 100,
    }
    if params:
        base_params.update(params)

    all_opps = []
    page = 1

    while True:
        base_params['page'] = page
        data = _ghl_get('opportunities/search', base_params)
        if not data:
            break

        opps = data.get('opportunities', [])
        all_opps.extend(opps)

        meta = data.get('meta', {})
        total = meta.get('total', 0)

        if len(all_opps) >= total or not opps:
            break
        page += 1

    return all_opps


def _is_rana_opportunity(opp):
    """Check if an opportunity is assigned to Rana."""
    assigned = opp.get('assignedTo', '')
    contact = opp.get('contact', {})
    # Check multiple fields for Rana's identifier
    if RANA_EMAIL in str(assigned).lower():
        return True
    if RANA_EMAIL in str(contact.get('email', '')).lower():
        return True
    return False


def _filter_by_date_range(opportunities, start_date, end_date):
    """Filter opportunities by creation date within the week."""
    filtered = []
    start_str = str(start_date)
    end_str = str(end_date)

    for opp in opportunities:
        created = opp.get('dateAdded', opp.get('createdAt', ''))
        if created:
            created_date = created[:10]  # YYYY-MM-DD
            if start_str <= created_date <= end_str:
                filtered.append(opp)

    return filtered


def _filter_by_stage_change_date(opportunities, start_date, end_date):
    """Filter opportunities by lastStageChangeAt within the date range.

    This captures when someone actually ENTERED a stage during the week,
    rather than counting everyone currently sitting in that stage.
    """
    filtered = []
    start_str = str(start_date)
    end_str = str(end_date)

    for opp in opportunities:
        stage_change = opp.get('lastStageChangeAt', '')
        if stage_change:
            change_date = stage_change[:10]  # YYYY-MM-DD
            if start_str <= change_date <= end_str:
                filtered.append(opp)

    return filtered


def _load_from_cache(week_ending_date):
    """Load pre-fetched GHL data from cache file (populated via MCP tools).

    Returns dict with weekly metrics if cache exists, None otherwise.
    """
    if not os.path.exists(CACHE_FILE):
        return None

    try:
        with open(CACHE_FILE, 'r') as f:
            cache = json.load(f)

        metrics = cache.get('weekly_metrics', {})
        result = {
            'week_ending_date': str(week_ending_date),
            'new_leads': metrics.get('new_leads', 0),
            'booked_appointments': metrics.get('booked_appointments', 0),
            'showed_appointments': metrics.get('showed_appointments', 0),
            'closed_deals': metrics.get('closed_deals', 0),
            'close_rate_overall': metrics.get('close_rate_overall', 0.0),
            'close_rate_rana': metrics.get('close_rate_rana', 0.0),
            'pipeline_value': metrics.get('pipeline_value', 0.0),
            'revenue_per_call': metrics.get('revenue_per_call', 0.0),
            'active_students': metrics.get('active_students', 0),
            'enrollment_growth_rate': metrics.get('enrollment_growth_rate', 0.0),
            'student_churn_rate': metrics.get('student_churn_rate', 0.0),
            'revenue_per_student': metrics.get('revenue_per_student', 0.0),
        }

        logger.info(f"Loaded GHL data from cache (fetched: {cache.get('fetched_at', 'unknown')})")
        return result
    except (json.JSONDecodeError, KeyError, IOError) as e:
        logger.error(f"Failed to load GHL cache: {e}")
        return None


def _collect_via_v1_api(week_ending_date):
    """Collect GHL metrics using the v1 REST API (Location API key auth).

    v1 endpoints used:
      GET /v1/pipelines/                              → pipeline & stage IDs
      GET /v1/pipelines/{id}/opportunities             → all opportunities
      GET /v1/opportunities/{id}                       → individual opp details
    """
    start_date = week_ending_date - timedelta(days=6)

    # ── Test connectivity ────────────────────────────────────────────────
    pipelines = _ghl_v1_get('pipelines/')
    if not pipelines:
        return None

    logger.info("GHL v1 API connected — fetching live data")

    # ── Sales Pipeline opportunities (limited — 15K+ total, only need recent) ──
    # Fetch first 10 pages (200 opps) — sorted by most recent, enough for weekly metrics
    sales_opps = _ghl_v1_get_all_opportunities(SALES_PIPELINE_ID, max_pages=10)
    logger.info(f"Sales pipeline: {len(sales_opps)} total opportunities")

    # Filter by date for weekly metrics
    weekly_new_leads = 0
    weekly_booked = 0
    weekly_showed = 0
    weekly_closed = 0
    weekly_closed_rana = 0
    weekly_showed_rana = 0
    pipeline_value = 0.0

    start_str = str(start_date)
    end_str = str(week_ending_date)

    for opp in sales_opps:
        stage_id = opp.get('pipelineStageId', '')
        created = (opp.get('dateAdded') or opp.get('createdAt') or '')[:10]
        last_stage = (opp.get('lastStageChangeAt') or '')[:10]
        monetary = float(opp.get('monetaryValue') or 0)

        # Count leads created this week
        if created and start_str <= created <= end_str:
            if stage_id in LEAD_STAGES:
                weekly_new_leads += 1

        # Count stage changes this week
        if last_stage and start_str <= last_stage <= end_str:
            if stage_id in BOOKED_STAGES:
                weekly_booked += 1
            elif stage_id in SHOWED_STAGES:
                weekly_showed += 1
                if _is_rana_opportunity(opp):
                    weekly_showed_rana += 1
            elif stage_id in CLOSED_STAGES:
                weekly_closed += 1
                pipeline_value += monetary
                if _is_rana_opportunity(opp):
                    weekly_closed_rana += 1

    close_rate = weekly_closed / weekly_showed if weekly_showed > 0 else 0.0
    close_rate_rana = weekly_closed_rana / weekly_showed_rana if weekly_showed_rana > 0 else 0.0
    rev_per_call = pipeline_value / weekly_showed if weekly_showed > 0 else 0.0

    # ── Student Success Pipeline (paginated, all pages — ~340 records) ──
    student_opps = _ghl_v1_get_all_opportunities(STUDENT_PIPELINE_ID, max_pages=25)

    active_students = 0
    graduated = 0
    churned = 0

    for opp in student_opps:
        stage_id = opp.get('pipelineStageId', '')
        if stage_id in ACTIVE_STUDENT_STAGES:
            active_students += 1
        elif stage_id in GRADUATED_STAGES:
            graduated += 1
        elif stage_id in CHURNED_STAGES:
            churned += 1

    total_ever = active_students + graduated + churned
    churn_rate = churned / total_ever if total_ever > 0 else 0.0

    logger.info(f"Student pipeline: {active_students} active, {graduated} graduated, {churned} churned")

    # ── Build result ─────────────────────────────────────────────────────
    result = {
        'week_ending_date': str(week_ending_date),
        'new_leads': weekly_new_leads,
        'booked_appointments': weekly_booked,
        'showed_appointments': weekly_showed,
        'closed_deals': weekly_closed,
        'close_rate_overall': round(close_rate, 3),
        'close_rate_rana': round(close_rate_rana, 3),
        'pipeline_value': round(pipeline_value, 2),
        'revenue_per_call': round(rev_per_call, 2),
        'active_students': active_students,
        'enrollment_growth_rate': 0.0,  # Need previous week to calculate
        'student_churn_rate': round(churn_rate, 3),
        'revenue_per_student': 0.0,  # Calculated in cross_platform
    }

    # ── Update cache with fresh data ─────────────────────────────────────
    cache_data = {
        'fetched_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'week_ending_date': str(week_ending_date),
        'sales_pipeline_total_open': len(sales_opps),
        'student_pipeline_total': len(student_opps),
        'weekly_metrics': result,
        'stage_breakdown': {
            'student_active': active_students,
            'student_graduated': graduated,
            'student_churned': churned,
        }
    }
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache_data, f, indent=2)
        logger.info("GHL cache updated with fresh API data")
    except IOError as e:
        logger.warning(f"Could not update GHL cache: {e}")

    return result


def collect_weekly_data(week_ending_date=None):
    """
    Collect all GHL metrics for a given week.

    Priority:
    1. Live v1 API (Location API key — most reliable)
    2. ghl_cache.json fallback (populated via MCP or previous API call)
    3. Zeros with warning

    Returns:
        dict with all GHL weekly metrics
    """
    if week_ending_date is None:
        today = datetime.utcnow().date()
        days_since_sunday = (today.weekday() + 1) % 7
        week_ending_date = today - timedelta(days=days_since_sunday)

    logger.info(f"Collecting GHL data for week ending {week_ending_date}")

    # ── Try live v1 API first ────────────────────────────────────────────
    if GHL_API_KEY:
        live_data = _collect_via_v1_api(week_ending_date)
        if live_data:
            logger.info(f"GHL live: {live_data['new_leads']} leads, "
                        f"{live_data['booked_appointments']} booked, "
                        f"{live_data['active_students']} active students")
            return live_data
        logger.warning("GHL v1 API failed — falling back to cache")

    # ── Fallback to MCP-populated cache ──────────────────────────────────
    cached = _load_from_cache(week_ending_date)
    if cached:
        logger.info(f"GHL cache loaded: {cached.get('new_leads', 0)} leads, "
                    f"{cached.get('booked_appointments', 0)} booked, "
                    f"{cached.get('active_students', 0)} active students")
        return cached

    # ── No data available ────────────────────────────────────────────────
    logger.error("No GHL data available (API failed and no cache). Returning zeros.")
    return {
        'week_ending_date': str(week_ending_date),
        'new_leads': 0, 'booked_appointments': 0, 'showed_appointments': 0,
        'closed_deals': 0, 'close_rate_overall': 0.0, 'close_rate_rana': 0.0,
        'pipeline_value': 0.0, 'revenue_per_call': 0.0, 'active_students': 0,
        'enrollment_growth_rate': 0.0, 'student_churn_rate': 0.0, 'revenue_per_student': 0.0,
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2))
