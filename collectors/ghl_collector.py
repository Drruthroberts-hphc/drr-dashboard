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


def _ghl_get(endpoint, params=None):
    """Make an authenticated GET request to the GHL API."""
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


def collect_weekly_data(week_ending_date=None):
    """
    Collect all GHL metrics for a given week.

    Falls back to cached data (from MCP tools) if the GHL API returns errors
    (e.g., Invalid JWT due to broken location API keys).

    Returns:
        dict with all GHL weekly metrics
    """
    if week_ending_date is None:
        today = datetime.utcnow().date()
        days_since_sunday = (today.weekday() + 1) % 7
        week_ending_date = today - timedelta(days=days_since_sunday)

    week_start = week_ending_date - timedelta(days=6)

    logger.info(f"Collecting GHL data for week {week_start} to {week_ending_date}")

    # ── Sales Pipeline Data ──────────────────────────────────────────────
    logger.info("Fetching sales pipeline opportunities...")
    sales_opps = _ghl_search_opportunities(SALES_PIPELINE_ID)

    # If API failed (returns empty due to auth error), try cache fallback
    if not sales_opps:
        logger.warning("GHL API returned no data (likely auth error). Trying cache fallback...")
        cached = _load_from_cache(week_ending_date)
        if cached:
            logger.info(f"Using cached GHL data: {cached.get('new_leads', 0)} leads, "
                        f"{cached.get('active_students', 0)} active students")
            return cached
        logger.error("No GHL cache available either. Returning zeros.")
        return {
            'week_ending_date': str(week_ending_date),
            'new_leads': 0, 'booked_appointments': 0, 'showed_appointments': 0,
            'closed_deals': 0, 'close_rate_overall': 0.0, 'close_rate_rana': 0.0,
            'pipeline_value': 0.0, 'revenue_per_call': 0.0, 'active_students': 0,
            'enrollment_growth_rate': 0.0, 'student_churn_rate': 0.0, 'revenue_per_student': 0.0,
        }

    logger.info(f"Total sales pipeline opportunities: {len(sales_opps)}")

    # Filter to this week's new entries
    weekly_opps = _filter_by_date_range(sales_opps, week_start, week_ending_date)

    # Count by stage
    stage_counts = defaultdict(int)
    for opp in weekly_opps:
        stage_id = opp.get('pipelineStageId', '')
        stage_counts[stage_id] += 1

    new_leads = sum(stage_counts.get(s, 0) for s in LEAD_STAGES)
    booked_appointments = sum(stage_counts.get(s, 0) for s in BOOKED_STAGES)
    showed_appointments = sum(stage_counts.get(s, 0) for s in SHOWED_STAGES)
    closed_deals = sum(stage_counts.get(s, 0) for s in CLOSED_STAGES)

    # Also count based on current stage (not just created this week)
    # For "booked" and "showed", we want opportunities that moved to these stages this week
    # GHL doesn't easily expose stage change history, so we use dateAdded as proxy
    all_weekly_booked = [o for o in weekly_opps if o.get('pipelineStageId') in BOOKED_STAGES]
    all_weekly_showed = [o for o in weekly_opps if o.get('pipelineStageId') in SHOWED_STAGES]
    all_weekly_closed = [o for o in weekly_opps if o.get('pipelineStageId') in CLOSED_STAGES]

    # Close rate
    close_rate_overall = (closed_deals / booked_appointments) if booked_appointments > 0 else 0.0

    # Rana's close rate
    rana_closed = sum(1 for o in all_weekly_closed if _is_rana_opportunity(o))
    rana_booked = sum(1 for o in all_weekly_booked if _is_rana_opportunity(o))
    close_rate_rana = (rana_closed / rana_booked) if rana_booked > 0 else 0.0

    # Pipeline value (sum of monetary values for open opportunities)
    pipeline_value = sum(
        float(opp.get('monetaryValue', 0) or 0)
        for opp in sales_opps
        if opp.get('status', '').lower() == 'open'
    )

    # Revenue per call
    total_closed_value = sum(
        float(opp.get('monetaryValue', 0) or 0)
        for opp in all_weekly_closed
    )
    revenue_per_call = (total_closed_value / showed_appointments) if showed_appointments > 0 else 0.0

    # ── Student Success Pipeline ─────────────────────────────────────────
    logger.info("Fetching student success pipeline...")
    student_opps = _ghl_search_opportunities(STUDENT_PIPELINE_ID)
    logger.info(f"Total student pipeline opportunities: {len(student_opps)}")

    # Active students (in active stages)
    active_students = sum(
        1 for opp in student_opps
        if opp.get('pipelineStageId') in ACTIVE_STUDENT_STAGES
    )

    # Graduated students
    graduated = sum(
        1 for opp in student_opps
        if opp.get('pipelineStageId') in GRADUATED_STAGES
    )

    # Churned students (this week)
    churned_this_week = sum(
        1 for opp in _filter_by_date_range(student_opps, week_start, week_ending_date)
        if opp.get('pipelineStageId') in CHURNED_STAGES
    )

    # Enrollment growth (new students this week)
    new_students_this_week = len(_filter_by_date_range(
        [o for o in student_opps if o.get('pipelineStageId') in ACTIVE_STUDENT_STAGES],
        week_start, week_ending_date
    ))

    enrollment_growth_rate = (new_students_this_week / active_students) if active_students > 0 else 0.0
    student_churn_rate = (churned_this_week / active_students) if active_students > 0 else 0.0

    # Revenue per student (total student pipeline value / active students)
    student_revenue = sum(
        float(opp.get('monetaryValue', 0) or 0)
        for opp in student_opps
        if opp.get('pipelineStageId') in ACTIVE_STUDENT_STAGES
    )
    revenue_per_student = (student_revenue / active_students) if active_students > 0 else 0.0

    # ── Assemble results ─────────────────────────────────────────────────
    result = {
        'week_ending_date': str(week_ending_date),
        'new_leads': new_leads,
        'booked_appointments': booked_appointments,
        'showed_appointments': showed_appointments,
        'closed_deals': closed_deals,
        'close_rate_overall': round(close_rate_overall, 4),
        'close_rate_rana': round(close_rate_rana, 4),
        'pipeline_value': round(pipeline_value, 2),
        'revenue_per_call': round(revenue_per_call, 2),
        'active_students': active_students,
        'enrollment_growth_rate': round(enrollment_growth_rate, 4),
        'student_churn_rate': round(student_churn_rate, 4),
        'revenue_per_student': round(revenue_per_student, 2),
    }

    logger.info(f"GHL collection complete: {new_leads} leads, {booked_appointments} booked, "
                f"{closed_deals} closed, {active_students} active students")

    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2))
