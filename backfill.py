"""
DRR Dashboard - Historical Backfill
=====================================
Runs the pipeline for each week from a start date to an end date,
populating Google Sheets and snapshots with historical data.

Usage:
    python backfill.py                          # Jan 4, 2026 through most recent Sunday
    python backfill.py --start 2026-02-01       # Custom start (snaps to next Sunday)
    python backfill.py --end 2026-02-28         # Custom end (snaps to previous Sunday)
    python backfill.py --dry-run                # Collect only, don't write sheets
    python backfill.py --skip-email             # Skip email sends (default for backfill)

Note:
    - GHL data uses cache fallback for all weeks (API JWT issue)
    - Social/YouTube subscriber counts are point-in-time, not historical
    - Emails are skipped by default during backfill (use --send-email to override)
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, date

logger = logging.getLogger(__name__)


def _next_sunday(d):
    """Return the next Sunday on or after date d."""
    days_ahead = 6 - d.weekday()  # weekday: Mon=0 ... Sun=6
    if days_ahead < 0:
        days_ahead += 7
    return d + timedelta(days=days_ahead)


def _prev_sunday(d):
    """Return the most recent Sunday on or before date d."""
    days_back = (d.weekday() + 1) % 7
    return d - timedelta(days=days_back)


def _all_sundays(start_date, end_date):
    """Generate all Sunday dates between start_date and end_date inclusive."""
    current = _next_sunday(start_date)
    while current <= end_date:
        yield current
        current += timedelta(days=7)


def run_backfill(start_date, end_date, dry_run=False, send_email=False):
    """
    Run the pipeline for each week from start_date to end_date.

    Args:
        start_date: date - beginning of backfill range
        end_date: date - end of backfill range
        dry_run: bool - if True, collect data but skip writes
        send_email: bool - if True, send emails (default False for backfill)
    """
    # Import here to avoid circular imports and allow --help without loading everything
    from main import run_pipeline

    sundays = list(_all_sundays(start_date, end_date))

    if not sundays:
        logger.error(f"No Sundays found between {start_date} and {end_date}")
        return

    logger.info("=" * 60)
    logger.info(f"DRR Dashboard Backfill")
    logger.info(f"  Range: {start_date} to {end_date}")
    logger.info(f"  Weeks to process: {len(sundays)}")
    logger.info(f"  First week ending: {sundays[0]}")
    logger.info(f"  Last week ending:  {sundays[-1]}")
    logger.info(f"  Dry run: {dry_run}")
    logger.info(f"  Send emails: {send_email}")
    logger.info("=" * 60)

    results = []
    failed = []

    for i, sunday in enumerate(sundays, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"[{i}/{len(sundays)}] Processing week ending {sunday}")
        logger.info(f"{'='*60}")

        try:
            result = run_pipeline(
                week_ending_date=sunday,
                dry_run=dry_run,
                overwrite=True,  # Always overwrite during backfill
                skip_email=not send_email,
            )

            revenue = result.get('cross_platform', {}).get('total_revenue', 0)
            alerts_count = len(result.get('alerts', []))
            results.append({
                'week': str(sunday),
                'revenue': revenue,
                'alerts': alerts_count,
                'status': 'ok',
            })

            logger.info(f"✓ Week {sunday}: ${revenue:,.2f} revenue, {alerts_count} alerts")

        except Exception as e:
            logger.error(f"✗ Week {sunday} FAILED: {e}")
            failed.append(str(sunday))
            results.append({
                'week': str(sunday),
                'revenue': 0,
                'alerts': 0,
                'status': f'failed: {e}',
            })

        # Small delay between weeks to avoid rate limits
        if i < len(sundays):
            logger.info("Waiting 3s before next week...")
            time.sleep(3)

    # ── Summary ────────────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info(f"  Weeks processed: {len(results)}")
    logger.info(f"  Successful: {len(results) - len(failed)}")
    logger.info(f"  Failed: {len(failed)}")

    if failed:
        logger.warning(f"  Failed weeks: {', '.join(failed)}")

    logger.info("\nWeekly summary:")
    for r in results:
        status = "✓" if r['status'] == 'ok' else "✗"
        logger.info(f"  {status} {r['week']}: ${r['revenue']:,.2f}")

    total_revenue = sum(r['revenue'] for r in results)
    logger.info(f"\n  Total revenue across all weeks: ${total_revenue:,.2f}")
    logger.info("=" * 60)

    return results


def main():
    parser = argparse.ArgumentParser(description='DRR Dashboard Historical Backfill')
    parser.add_argument(
        '--start',
        default='2026-01-01',
        help='Start date (YYYY-MM-DD, default: 2026-01-01)',
    )
    parser.add_argument(
        '--end',
        help='End date (YYYY-MM-DD, default: most recent Sunday)',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Collect data but skip sheet writes and emails',
    )
    parser.add_argument(
        '--send-email',
        action='store_true',
        help='Send emails for each week (disabled by default during backfill)',
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

    # Parse dates
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    except ValueError:
        logger.error(f"Invalid start date: {args.start}. Use YYYY-MM-DD.")
        sys.exit(1)

    if args.end:
        try:
            end_date = datetime.strptime(args.end, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid end date: {args.end}. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        # Default to most recent Sunday
        today = date.today()
        end_date = _prev_sunday(today)

    if start_date > end_date:
        logger.error(f"Start date {start_date} is after end date {end_date}")
        sys.exit(1)

    # Run backfill
    try:
        run_backfill(
            start_date=start_date,
            end_date=end_date,
            dry_run=args.dry_run,
            send_email=args.send_email,
        )
    except KeyboardInterrupt:
        logger.info("\nBackfill interrupted by user")
        sys.exit(1)


if __name__ == '__main__':
    main()
