"""
DRR Dashboard - Main Orchestrator
===================================
Weekly pipeline: collect → calculate → store → alert → generate dashboard.

Usage:
    python main.py                  # Run for most recent completed week (last Sunday)
    python main.py --date 2026-03-01  # Run for a specific week-ending date
    python main.py --dry-run         # Collect data but skip writes/emails
    python main.py --overwrite       # Overwrite existing sheet data
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta

# ── Imports ───────────────────────────────────────────────────────────────
from collectors.shopify_collector import collect_weekly_data as collect_shopify
from collectors.klaviyo_collector import collect_weekly_data as collect_klaviyo
from collectors.stripe_collector import collect_weekly_data as collect_stripe
from collectors.ghl_collector import collect_weekly_data as collect_ghl
from collectors.social_collector import collect_weekly_data as collect_social

from cross_platform import calculate_cross_platform
from sheets_writer import write_all_weekly_data, write_alert, get_previous_week_data
from alerts import check_thresholds, send_alert_email, send_weekly_summary
from dashboard_generator import generate_dashboard, save_dashboard

logger = logging.getLogger(__name__)


def _load_snapshot_history():
    """Load all weekly snapshots sorted by date for trend charts."""
    snapshot_dir = os.path.join(os.path.dirname(__file__), 'snapshots')
    if not os.path.isdir(snapshot_dir):
        return []

    snapshots = []
    for fname in sorted(os.listdir(snapshot_dir)):
        if fname.startswith('snapshot_') and fname.endswith('.json'):
            fpath = os.path.join(snapshot_dir, fname)
            try:
                with open(fpath, 'r') as f:
                    snap = json.load(f)
                snapshots.append(snap)
            except Exception as e:
                logger.warning(f"Could not load snapshot {fname}: {e}")

    # Sort by week_ending_date
    snapshots.sort(key=lambda s: s.get('week_ending_date', ''))
    return snapshots


def _default_week_ending():
    """Return most recent Sunday as date object."""
    today = datetime.utcnow().date()
    days_since_sunday = (today.weekday() + 1) % 7
    if days_since_sunday == 0:
        days_since_sunday = 7  # If today is Sunday, use last Sunday
    return today - timedelta(days=days_since_sunday)


def run_pipeline(week_ending_date=None, dry_run=False, overwrite=False, skip_email=False):
    """
    Execute the full weekly data pipeline.

    Args:
        week_ending_date: date object for the week ending (Sunday).
        dry_run: If True, collect data but don't write to sheets or send emails.
        overwrite: If True, overwrite existing sheet rows for this week.
        skip_email: If True, skip email sends (useful for backfill).

    Returns:
        dict with all collected data and results.
    """
    if week_ending_date is None:
        week_ending_date = _default_week_ending()

    logger.info("=" * 60)
    logger.info(f"DRR Dashboard Pipeline - Week ending {week_ending_date}")
    logger.info("=" * 60)

    # ── Phase 1: Collect from all platforms ────────────────────────────────
    all_data = {}

    collectors = [
        ('shopify', collect_shopify, 'Shopify'),
        ('klaviyo', collect_klaviyo, 'Klaviyo'),
        ('stripe', collect_stripe, 'Stripe'),
        ('ghl', collect_ghl, 'GoHighLevel'),
        ('social', collect_social, 'Social Media'),
    ]

    for key, collector_fn, display_name in collectors:
        logger.info(f"--- Collecting {display_name} ---")
        try:
            data = collector_fn(week_ending_date)
            all_data[key] = data
            logger.info(f"{display_name}: OK ({len(data)} metrics)")
        except Exception as e:
            logger.error(f"{display_name} collection FAILED: {e}")
            all_data[key] = {}

    # ── Phase 2: Get previous week data for WoW calculations ──────────────
    previous_cross = None
    if not dry_run:
        try:
            previous_cross = get_previous_week_data(
                'CrossPlatform_Weekly', week_ending_date
            )
            if previous_cross:
                logger.info(f"Previous week data found: ${previous_cross.get('total_revenue', '?')}")
            else:
                logger.info("No previous week data found (first run or new sheet)")
        except Exception as e:
            logger.warning(f"Could not fetch previous week data: {e}")

    # ── Phase 3: Calculate cross-platform metrics ─────────────────────────
    logger.info("--- Calculating cross-platform metrics ---")
    cross_data = calculate_cross_platform(all_data, previous_cross)
    all_data['cross_platform'] = cross_data
    logger.info(f"Total revenue: ${cross_data.get('total_revenue', 0):,.2f}")

    # ── Phase 4: Check alert thresholds ───────────────────────────────────
    logger.info("--- Checking alert thresholds ---")
    # Merge cross-platform data into all_data for threshold checking
    alert_check_data = dict(all_data)
    alert_check_data['cross_platform'] = cross_data
    alerts = check_thresholds(alert_check_data)

    if alerts:
        logger.warning(f"{len(alerts)} alert(s) triggered!")
        for a in alerts:
            logger.warning(f"  - {a['metric_name']}: {a['current_value']}")
    else:
        logger.info("No alerts triggered")

    # ── Phase 5: Write to Google Sheets ───────────────────────────────────
    if not dry_run:
        logger.info("--- Writing to Google Sheets ---")
        try:
            sheet_results = write_all_weekly_data(
                shopify_data=all_data.get('shopify', {}),
                klaviyo_data=all_data.get('klaviyo', {}),
                ghl_data=all_data.get('ghl', {}),
                stripe_data=all_data.get('stripe', {}),
                social_data=all_data.get('social', {}),
                cross_platform_data=cross_data,
                overwrite=overwrite,
            )
            for tab, status in sheet_results.items():
                logger.info(f"  {tab}: {status}")

            # Write alerts to log
            for alert in alerts:
                write_alert(alert)

        except Exception as e:
            logger.error(f"Google Sheets write FAILED: {e}")
    else:
        logger.info("--- DRY RUN: Skipping Google Sheets write ---")

    # ── Phase 6: Send emails ──────────────────────────────────────────────
    if not dry_run and not skip_email:
        logger.info("--- Sending emails ---")

        # Send alert email if any thresholds breached
        if alerts:
            try:
                sent = send_alert_email(alerts, str(week_ending_date))
                logger.info(f"Alert email: {'sent' if sent else 'failed'}")
            except Exception as e:
                logger.error(f"Alert email FAILED: {e}")

        # Send weekly summary
        try:
            sent = send_weekly_summary(all_data, alerts, str(week_ending_date))
            logger.info(f"Weekly summary email: {'sent' if sent else 'failed'}")
        except Exception as e:
            logger.error(f"Weekly summary email FAILED: {e}")
    elif skip_email:
        logger.info("--- Skipping email sends (backfill mode) ---")
    else:
        logger.info("--- DRY RUN: Skipping email sends ---")

    # ── Phase 7: Generate HTML dashboard ──────────────────────────────────
    logger.info("--- Generating HTML dashboard ---")
    try:
        # Get previous week data for WoW displays in dashboard
        previous_data = {
            'shopify': None,
            'klaviyo': None,
            'stripe': None,
            'ghl': None,
            'social': None,
        }
        if not dry_run:
            for key, tab in [
                ('shopify', 'Shopify_Weekly'),
                ('klaviyo', 'Klaviyo_Weekly'),
                ('stripe', 'Stripe_Weekly'),
                ('ghl', 'GHL_Weekly'),
                ('social', 'Social_Weekly'),
            ]:
                try:
                    previous_data[key] = get_previous_week_data(tab, week_ending_date)
                except Exception:
                    pass

        # Load all historical snapshots for trend charts
        history = _load_snapshot_history()
        logger.info(f"Loaded {len(history)} historical snapshots for trend charts")

        html = generate_dashboard(
            all_data=all_data,
            cross_data=cross_data,
            alerts=alerts,
            previous_data=previous_data,
            week_ending_date=str(week_ending_date),
            history=history,
        )
        save_dashboard(html)
        logger.info("Dashboard saved to dashboard/index.html")
    except Exception as e:
        logger.error(f"Dashboard generation FAILED: {e}")

    # ── Summary ───────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Pipeline complete!")
    logger.info(f"  Week ending: {week_ending_date}")
    logger.info(f"  Total revenue: ${cross_data.get('total_revenue', 0):,.2f}")
    logger.info(f"  Alerts: {len(alerts)}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info("=" * 60)

    # Save raw data snapshot for debugging
    snapshot_dir = os.path.join(os.path.dirname(__file__), 'snapshots')
    os.makedirs(snapshot_dir, exist_ok=True)
    snapshot_path = os.path.join(snapshot_dir, f"snapshot_{week_ending_date}.json")
    try:
        with open(snapshot_path, 'w') as f:
            json.dump({
                'week_ending_date': str(week_ending_date),
                'all_data': all_data,
                'cross_platform': cross_data,
                'alerts': alerts,
            }, f, indent=2, default=str)
        logger.info(f"Snapshot saved: {snapshot_path}")
    except Exception as e:
        logger.warning(f"Could not save snapshot: {e}")

    return {
        'week_ending_date': str(week_ending_date),
        'all_data': all_data,
        'cross_platform': cross_data,
        'alerts': alerts,
    }


def main():
    parser = argparse.ArgumentParser(description='DRR Dashboard Weekly Pipeline')
    parser.add_argument(
        '--date',
        help='Week ending date (YYYY-MM-DD, must be a Sunday)',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Collect data but skip sheet writes and emails',
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing sheet data for this week',
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable debug logging',
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)-8s %(name)-20s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Parse date
    week_ending_date = None
    if args.date:
        try:
            week_ending_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD.")
            sys.exit(1)

    # Run
    try:
        result = run_pipeline(
            week_ending_date=week_ending_date,
            dry_run=args.dry_run,
            overwrite=args.overwrite,
        )
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
