"""
Coaching Pipeline Collector
============================
Tracks the high-ticket coaching funnel separately from e-commerce.

Pulls from GHL contact tags:
  - 'application form submitted'  → top of funnel
  - 'qualified' / 'qualified-app'  → passed application screening
  - 'not qualified' / 'disqualified-app' → screened out
  - 'live-call-apr30-applicant' (or other dated variants) → webinar attendees
  - 'hphc enrolled' → closed deal

Metrics produced (rolling, not weekly — coaching deals span weeks):
  applications_total, applications_4w, applications_2w,
  qualified, not_qualified, qualification_rate,
  webinar_registrants (most recent),
  enrollments_total, enrollments_4w
"""

import json
import logging
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

from config import GHL_API_KEY

logger = logging.getLogger(__name__)

V1_BASE = 'https://rest.gohighlevel.com/v1'

# The new long-form HPHC application launched on this date.
# Submissions before this came from a different (shorter) form and should be
# excluded from the coaching pipeline tile.
APPLICATION_FORM_LAUNCH_DATE = '2026-04-24'

# Tag keys we look for (lower-case, exact match)
TAG_APPLIED = 'application form submitted'
TAG_QUALIFIED = {'qualified', 'qualified-app'}
TAG_NOT_QUALIFIED = {'not qualified', 'disqualified-app'}
TAG_ENROLLED = {'hphc enrolled', 'enrolled student tag', 'hphc-enrolled'}


def _gh_get(url):
    req = urllib.request.Request(url, headers={
        'Authorization': f'Bearer {GHL_API_KEY}',
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 DRR-Dashboard/1.0',
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        logger.error(f"GHL coaching collector error {e.code}: {e.read().decode()[:200]}")
        return None


def _fetch_recent_contacts(stop_before_date='2026-01-01', max_pages=30):
    """Fetch recent contacts (most-recent-first) until we pass the cutoff date.

    Stops at max_pages or when contacts older than stop_before_date appear.
    """
    url = f"{V1_BASE}/contacts/?limit=100"
    all_contacts = []
    seen = set()

    for page in range(max_pages):
        if page > 0:
            time.sleep(0.4)
        r = _gh_get(url.replace('http://', 'https://'))
        if not r or 'contacts' not in r:
            break

        contacts = r['contacts']
        new_added = 0
        for c in contacts:
            if c['id'] not in seen:
                seen.add(c['id'])
                all_contacts.append(c)
                new_added += 1

        if not new_added or not contacts:
            break

        # Stop once we've gone past the cutoff
        oldest_in_page = min((c.get('dateAdded', '9999')[:10] for c in contacts), default='9999')
        if oldest_in_page < stop_before_date:
            break

        next_url = r.get('meta', {}).get('nextPageUrl')
        if not next_url:
            break
        url = next_url

    return all_contacts


def _has_any_tag(contact, tag_set):
    """Return True if any tag (lowercased) is in tag_set."""
    contact_tags = {t.lower().strip() for t in contact.get('tags', [])}
    return bool(contact_tags & tag_set)


def collect_coaching_pipeline(week_ending_date=None):
    """
    Collect coaching/certification pipeline metrics.

    Returns a dict with applicant counts, qualification rates, enrollments,
    plus a list of recent applicants (last 4 weeks) for the dashboard tile.
    """
    if week_ending_date is None:
        today = datetime.utcnow().date()
        days_since_sunday = (today.weekday() + 1) % 7
        week_ending_date = today - timedelta(days=days_since_sunday)

    # Hard cutoff: only count applications from the new long-form launch date.
    # This is the date the user told us the application form was published.
    form_launch = datetime.strptime(APPLICATION_FORM_LAUNCH_DATE, '%Y-%m-%d').date()

    cutoff_4w = max(form_launch, week_ending_date - timedelta(days=28))
    cutoff_2w = max(form_launch, week_ending_date - timedelta(days=14))
    cutoff_1w = max(form_launch, week_ending_date - timedelta(days=6))

    logger.info(f"Coaching pipeline: collecting from form launch {form_launch}")

    # Always pull from form_launch (or 8 weeks back, whichever is later)
    fetch_from = min(form_launch, week_ending_date - timedelta(days=8))
    contacts = _fetch_recent_contacts(stop_before_date=str(fetch_from))
    logger.info(f"Coaching pipeline: scanned {len(contacts)} recent contacts")

    applied_all = []
    enrolled_all = []
    qualified_all = []
    not_qual_all = []

    for c in contacts:
        tags_lower = {t.lower().strip() for t in c.get('tags', [])}
        date_added = c.get('dateAdded', '')[:10]

        # Only count applicants from the new long-form launch date forward
        try:
            d_obj = datetime.strptime(date_added, '%Y-%m-%d').date()
        except ValueError:
            continue
        if d_obj < form_launch:
            continue

        if TAG_APPLIED in tags_lower:
            applied_all.append({
                'date': date_added,
                'name': (c.get('contactName')
                         or f"{c.get('firstName','')} {c.get('lastName','')}".strip()
                         or '(unknown)'),
                'email': c.get('email', ''),
                'tags': list(c.get('tags', [])),
                'is_qualified': bool(tags_lower & TAG_QUALIFIED),
                'is_disqualified': bool(tags_lower & TAG_NOT_QUALIFIED),
                'is_enrolled': bool(tags_lower & TAG_ENROLLED),
            })

        if tags_lower & TAG_ENROLLED:
            enrolled_all.append({
                'date': date_added,
                'name': (c.get('contactName')
                         or f"{c.get('firstName','')} {c.get('lastName','')}".strip()),
                'email': c.get('email', ''),
            })

        if tags_lower & TAG_QUALIFIED:
            qualified_all.append(c.get('id'))

        if tags_lower & TAG_NOT_QUALIFIED:
            not_qual_all.append(c.get('id'))

    # Filter applicants by time window
    def in_window(record, cutoff_date):
        try:
            d = datetime.strptime(record['date'], '%Y-%m-%d').date()
            return d >= cutoff_date
        except ValueError:
            return False

    applied_4w = [a for a in applied_all if in_window(a, cutoff_4w)]
    applied_2w = [a for a in applied_all if in_window(a, cutoff_2w)]
    applied_1w = [a for a in applied_all if in_window(a, cutoff_1w)]
    enrolled_4w = [e for e in enrolled_all if in_window(e, cutoff_4w)]

    qualification_rate = 0.0
    decided = len(qualified_all) + len(not_qual_all)
    if decided > 0:
        qualification_rate = len(qualified_all) / decided

    # Recent applicants table (last 4 weeks, sorted newest first)
    recent_applicants = sorted(applied_4w, key=lambda a: a['date'], reverse=True)

    # Build a webinar registrants count from any "live-call-*-applicant" tag
    webinar_tag_re_count = 0
    for c in contacts:
        for t in c.get('tags', []):
            tl = t.lower().strip()
            if tl.startswith('live-call-') and tl.endswith('-applicant'):
                webinar_tag_re_count += 1
                break

    result = {
        'week_ending_date': str(week_ending_date),
        'form_launch_date': str(form_launch),
        'applications_total': len(applied_all),  # since form launch
        'applications_since_launch': len(applied_all),
        'applications_4w': len(applied_4w),
        'applications_2w': len(applied_2w),
        'applications_1w': len(applied_1w),
        'qualified_count': len(qualified_all),
        'not_qualified_count': len(not_qual_all),
        'qualification_rate': round(qualification_rate, 3),
        'enrollments_total': len(enrolled_all),
        'enrollments_4w': len(enrolled_4w),
        'webinar_registrants': webinar_tag_re_count,
        'recent_applicants_json': json.dumps(recent_applicants),
        'recent_enrollments_json': json.dumps(
            sorted(enrolled_4w, key=lambda e: e['date'], reverse=True)
        ),
    }

    logger.info(
        f"Coaching pipeline: {result['applications_4w']} apps (4w), "
        f"{result['enrollments_4w']} enrollments (4w), "
        f"{result['webinar_registrants']} webinar registrants"
    )

    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_coaching_pipeline()
    print(json.dumps({k: v for k, v in data.items() if 'json' not in k}, indent=2))
    print('Recent applicants:')
    for a in json.loads(data['recent_applicants_json']):
        flags = []
        if a['is_qualified']: flags.append('QUALIFIED')
        if a['is_disqualified']: flags.append('DISQUALIFIED')
        if a['is_enrolled']: flags.append('ENROLLED')
        print(f"  {a['date']} | {a['name'][:30]:30} | {a['email'][:30]:30} | {','.join(flags)}")
