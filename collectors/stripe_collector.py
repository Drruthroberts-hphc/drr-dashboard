"""
Stripe Data Collector
=====================
Collects payment metrics: gross/net volume, success rates, disputes,
refunds, MRR, and processing fees from the Stripe API.
"""

import json
import logging
import time
from datetime import datetime, timedelta

import stripe

from config import STRIPE_API_KEY

logger = logging.getLogger(__name__)

stripe.api_key = STRIPE_API_KEY


def _ts(dt):
    """Convert a date to Unix timestamp."""
    return int(datetime.combine(dt, datetime.min.time()).timestamp())


def _ts_end(dt):
    """Convert a date to end-of-day Unix timestamp."""
    return int(datetime.combine(dt, datetime.max.time()).timestamp())


def collect_weekly_data(week_ending_date=None):
    """
    Collect all Stripe metrics for a given week.

    Returns:
        dict with all Stripe weekly metrics
    """
    if week_ending_date is None:
        today = datetime.utcnow().date()
        days_since_sunday = (today.weekday() + 1) % 7
        week_ending_date = today - timedelta(days=days_since_sunday)

    week_start = week_ending_date - timedelta(days=6)
    start_ts = _ts(week_start)
    end_ts = _ts_end(week_ending_date)

    logger.info(f"Collecting Stripe data for week {week_start} to {week_ending_date}")

    # ── Charges (payments) ───────────────────────────────────────────────
    gross_volume = 0.0
    net_volume = 0.0
    successful_charges = 0
    failed_charges = 0
    total_fees = 0.0

    try:
        charges = stripe.Charge.list(
            created={'gte': start_ts, 'lte': end_ts},
            limit=100,
        )

        for charge in charges.auto_paging_iter():
            amount = charge.amount / 100.0  # cents to dollars

            if charge.status == 'succeeded':
                gross_volume += amount
                successful_charges += 1

                # Get fees from balance transaction
                if charge.balance_transaction:
                    try:
                        bt = stripe.BalanceTransaction.retrieve(charge.balance_transaction)
                        fee = bt.fee / 100.0
                        total_fees += fee
                        net_volume += (bt.net / 100.0)
                    except Exception as e:
                        logger.warning(f"Could not fetch balance transaction: {e}")
                        net_volume += amount  # fallback
            elif charge.status == 'failed':
                failed_charges += 1

    except Exception as e:
        logger.error(f"Error fetching charges: {e}")

    total_charges = successful_charges + failed_charges
    payment_success_rate = (successful_charges / total_charges) if total_charges > 0 else 0.0

    # ── Refunds ──────────────────────────────────────────────────────────
    refund_count = 0
    refund_amount = 0.0

    try:
        refunds = stripe.Refund.list(
            created={'gte': start_ts, 'lte': end_ts},
            limit=100,
        )

        for refund in refunds.auto_paging_iter():
            if refund.status == 'succeeded':
                refund_count += 1
                refund_amount += refund.amount / 100.0

    except Exception as e:
        logger.error(f"Error fetching refunds: {e}")

    refund_rate = (refund_count / successful_charges) if successful_charges > 0 else 0.0

    # ── Disputes ─────────────────────────────────────────────────────────
    dispute_count = 0
    dispute_amount = 0.0

    try:
        disputes = stripe.Dispute.list(
            created={'gte': start_ts, 'lte': end_ts},
            limit=100,
        )

        for dispute in disputes.auto_paging_iter():
            dispute_count += 1
            dispute_amount += dispute.amount / 100.0

    except Exception as e:
        logger.error(f"Error fetching disputes: {e}")

    dispute_rate = (dispute_count / successful_charges) if successful_charges > 0 else 0.0

    # ── MRR (Monthly Recurring Revenue) ──────────────────────────────────
    mrr = 0.0

    def _safe_get(obj, key, default=None):
        """Safely .get() from StripeObject/dict/None — newer SDK versions
        return None instead of {} for missing fields, which breaks chained .get()."""
        if obj is None:
            return default
        if isinstance(obj, dict):
            v = obj.get(key, default)
        else:
            # StripeObject — try attribute then dict-style access
            v = getattr(obj, key, None)
            if v is None and hasattr(obj, '__getitem__'):
                try:
                    v = obj[key]
                except (KeyError, AttributeError, TypeError):
                    v = default
        return v if v is not None else default

    try:
        subscriptions = stripe.Subscription.list(status='active', limit=100)

        for sub in subscriptions.auto_paging_iter():
            items_obj = _safe_get(sub, 'items', {})
            items_data = _safe_get(items_obj, 'data', []) or []

            for item in items_data:
                price = _safe_get(item, 'price', {})
                amount = (_safe_get(price, 'unit_amount', 0) or 0) / 100.0
                recurring = _safe_get(price, 'recurring', {})
                interval = _safe_get(recurring, 'interval', 'month') or 'month'
                interval_count = _safe_get(recurring, 'interval_count', 1) or 1
                quantity = _safe_get(item, 'quantity', 1) or 1

                # Normalize to monthly
                if interval == 'year':
                    monthly = (amount * quantity) / (12 * interval_count)
                elif interval == 'month':
                    monthly = (amount * quantity) / interval_count
                elif interval == 'week':
                    monthly = (amount * quantity * 4.33) / interval_count
                elif interval == 'day':
                    monthly = (amount * quantity * 30.44) / interval_count
                else:
                    monthly = amount * quantity

                mrr += monthly

    except Exception as e:
        import traceback
        logger.error(f"Error calculating MRR: {type(e).__name__}: {e}")
        logger.debug(traceback.format_exc())

    # ── Assemble results ─────────────────────────────────────────────────
    result = {
        'week_ending_date': str(week_ending_date),
        'gross_payment_volume': round(gross_volume, 2),
        'net_revenue': round(net_volume, 2),
        'payment_success_rate': round(payment_success_rate, 4),
        'dispute_rate': round(dispute_rate, 6),
        'refund_rate': round(refund_rate, 4),
        'refund_amount': round(refund_amount, 2),
        'mrr': round(mrr, 2),
        'processing_fees': round(total_fees, 2),
    }

    logger.info(f"Stripe collection complete: ${gross_volume:.2f} gross, "
                f"{successful_charges} successful charges, MRR ${mrr:.2f}")

    return result


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    data = collect_weekly_data()
    print(json.dumps(data, indent=2))
