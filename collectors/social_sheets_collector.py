"""
Social Sheets Collector
========================
Pulls the two manually-maintained Google Sheets that the team updates weekly
and merges the most-recent row for the target week into the snapshot.

Sheets (Viewer access granted to drr-dashboard-service@drr-dashboard-489622.iam.gserviceaccount.com):

  1. Weekly Social Stats (aggregate by platform)
     ID: 1FtxKt1LuZxbZnwBG5vlut296LTIX9SKNtjIAAVhWzaE
     Columns: Week Ending (Sun) | Platform | Posts | Views/Impressions | Unique Viewers
              | Reach | Content Interactions | Link Clicks | Visits | New Follows
              | Unfollows | Net Follows | Followers (Lifetime) | Messaging Contacts | Notes

  2. Post-level FB/IG/LinkedIn export (raw)
     ID: 12j6TpP0TuEQmlensYwt8xXZYSe7lmAcTsT9HyQEk-NU
     Used for top-posts data per week (best-N by reach/views).
"""

import json
import logging
import os
from datetime import datetime, timedelta, date

logger = logging.getLogger(__name__)

WEEKLY_SHEET_ID = '1FtxKt1LuZxbZnwBG5vlut296LTIX9SKNtjIAAVhWzaE'
POST_LEVEL_SHEET_ID = '12j6TpP0TuEQmlensYwt8xXZYSe7lmAcTsT9HyQEk-NU'
PLATFORMS = ['Facebook', 'Instagram', 'LinkedIn']


def _get_sheets_client():
    """Return an authenticated gspread client using the service account credentials."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        logger.error("gspread or google-auth not installed — install with: pip install gspread google-auth")
        return None

    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cred_path = os.path.join(here, 'credentials.json')
    if not os.path.exists(cred_path):
        logger.warning(f"credentials.json not found at {cred_path}")
        return None

    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        creds = Credentials.from_service_account_file(cred_path, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Could not authenticate gspread client: {e}")
        return None


def _parse_int(v):
    """Coerce a value that might be '15,900' or '30.8K' to int."""
    if v is None or v == '':
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip().replace(',', '')
    if s.endswith('K') or s.endswith('k'):
        try:
            return int(float(s[:-1]) * 1000)
        except ValueError:
            return 0
    if s.endswith('M') or s.endswith('m'):
        try:
            return int(float(s[:-1]) * 1_000_000)
        except ValueError:
            return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def _parse_date(v):
    """Parse a date cell. Returns date or None."""
    if not v:
        return None
    if isinstance(v, (date, datetime)):
        return v.date() if isinstance(v, datetime) else v
    s = str(v).strip()
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%m/%d/%y', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _week_match(row_date, week_ending_date):
    """Match a row date to the target week (Sunday-ending).
    Row date may be the Sunday or any day within Mon-Sun of the same week."""
    if row_date is None:
        return False
    if row_date == week_ending_date:
        return True
    # Same week window check
    week_start = week_ending_date - timedelta(days=6)
    return week_start <= row_date <= week_ending_date


def collect_weekly_data(week_ending_date=None):
    """
    Read the Weekly Social Stats sheet and return a dict of the platform-level
    metrics for the target week, plus a top-posts blob per platform.
    """
    if week_ending_date is None:
        today = datetime.utcnow().date()
        days_since_sunday = (today.weekday() + 1) % 7
        if days_since_sunday == 0:
            days_since_sunday = 7
        week_ending_date = today - timedelta(days=days_since_sunday)

    if isinstance(week_ending_date, datetime):
        week_ending_date = week_ending_date.date()

    out = {
        'week_ending_date': str(week_ending_date),
        'source': 'manual_sheet',
        'platforms': {},
        'sheet_pulled_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
    }

    client = _get_sheets_client()
    if not client:
        logger.warning("Social sheets collector: no client — returning empty")
        return out

    # ── Weekly aggregate sheet ──────────────────────────────────────────
    try:
        sh = client.open_by_key(WEEKLY_SHEET_ID)
        ws = sh.sheet1  # First tab has the aggregate data
        rows = ws.get_all_records()
        logger.info(f"Social sheets: pulled {len(rows)} rows from Weekly Social Stats")
    except Exception as e:
        logger.error(f"Could not read Weekly Social Stats sheet: {e}")
        return out

    # Group rows by platform, then pick the one whose date matches target week
    for platform in PLATFORMS:
        platform_rows = [r for r in rows if str(r.get('Platform', '')).strip() == platform]
        match = None
        for r in platform_rows:
            row_date = _parse_date(r.get('Week Ending (Sun)') or r.get('Week Ending'))
            if _week_match(row_date, week_ending_date):
                match = r
                break

        if match:
            out['platforms'][platform.lower()] = {
                'posts': _parse_int(match.get('Posts')),
                'views': _parse_int(match.get('Views / Impressions') or match.get('Views/Impressions')),
                'unique_viewers': _parse_int(match.get('Unique Viewers')),
                'reach': _parse_int(match.get('Reach')),
                'interactions': _parse_int(match.get('Content Interactions')),
                'link_clicks': _parse_int(match.get('Link Clicks')),
                'visits': _parse_int(match.get('Visits')),
                'new_follows': _parse_int(match.get('New Follows')),
                'unfollows': _parse_int(match.get('Unfollows')),
                'net_follows': _parse_int(match.get('Net Follows')),
                'followers_lifetime': _parse_int(match.get('Followers (Lifetime)')),
                'messaging_contacts': _parse_int(match.get('Messaging Contacts')),
                'notes': str(match.get('Notes', '')).strip()[:300],
            }
        else:
            out['platforms'][platform.lower()] = {
                'posts': 0, 'views': 0, 'reach': 0, 'interactions': 0,
                'link_clicks': 0, 'new_follows': 0, 'followers_lifetime': 0,
                '_no_data_for_week': True,
            }
            logger.warning(f"No {platform} row found for week ending {week_ending_date}")

    # ── Summary rollup for cross-platform metrics ────────────────────────
    totals = {'views': 0, 'reach': 0, 'interactions': 0, 'link_clicks': 0,
              'new_follows': 0, 'posts': 0}
    for plat_data in out['platforms'].values():
        for k in totals:
            totals[k] += plat_data.get(k, 0)
    out['weekly_totals'] = totals

    logger.info(f"Social sheets: week {week_ending_date} — "
                f"{totals['views']:,} views, {totals['reach']:,} reach, "
                f"{totals['link_clicks']:,} clicks across {totals['posts']} posts")
    return out


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2, default=str))
