"""
Alert System
=============
Checks collected metrics against thresholds and sends email alerts.
Uses Gmail API for sending via Dr. Ruth's account.
"""

import json
import logging
import os
import base64
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config import ALERT_THRESHOLDS, ALERT_EMAILS, GMAIL_CREDENTIALS, GMAIL_TOKEN

logger = logging.getLogger(__name__)

GMAIL_SCOPES = ['https://www.googleapis.com/auth/gmail.send']


def _get_gmail_service():
    """Authenticate and return Gmail API service."""
    creds = None

    if os.path.exists(GMAIL_TOKEN):
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN, GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Don't open browser for OAuth - it blocks the pipeline.
            # Token must be obtained manually first via: python alerts.py --auth
            raise RuntimeError(
                "No valid Gmail token. Run 'python alerts.py --auth' to authorize, "
                "or add your email as a test user in Google Cloud Console first."
            )

        with open(GMAIL_TOKEN, 'w') as f:
            f.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)


def check_thresholds(all_data):
    """
    Check all collected data against defined thresholds.

    Args:
        all_data: dict with keys like 'shopify', 'klaviyo', 'stripe', etc.
                  Each value is the collector's result dict.

    Returns:
        list of triggered alerts, each a dict with:
          metric_name, current_value, threshold, direction, platform, status
    """
    triggered = []

    # Flatten all metrics into one dict for easy lookup
    flat_metrics = {}
    for source, data in all_data.items():
        if isinstance(data, dict):
            flat_metrics.update(data)

    for alert_def in ALERT_THRESHOLDS:
        metric = alert_def['metric']
        threshold = alert_def['threshold']
        direction = alert_def['direction']
        display_name = alert_def['display_name']
        platform = alert_def['platform']

        current_value = flat_metrics.get(metric)
        if current_value is None:
            continue

        try:
            current_value = float(current_value)
        except (ValueError, TypeError):
            continue

        is_triggered = False
        if direction == 'above' and current_value > threshold:
            is_triggered = True
        elif direction == 'below' and current_value < threshold:
            is_triggered = True

        if is_triggered:
            triggered.append({
                'timestamp': datetime.utcnow().isoformat(),
                'metric_name': display_name,
                'current_value': current_value,
                'threshold': threshold,
                'direction': direction,
                'status': 'TRIGGERED',
                'notified': 'pending',
                'platform': platform,
            })
            logger.warning(f"ALERT: {display_name} = {current_value} "
                          f"(threshold: {direction} {threshold})")

    return triggered


def _format_alert_email(alerts, week_ending_date):
    """Format alerts into an HTML email body."""
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <div style="background: #d32f2f; color: white; padding: 15px; border-radius: 5px 5px 0 0;">
            <h2 style="margin: 0;">DRR Dashboard Alert</h2>
            <p style="margin: 5px 0 0 0;">Week ending: {week_ending_date}</p>
        </div>
        <div style="padding: 20px; border: 1px solid #ddd; border-top: none;">
            <p><strong>{len(alerts)} threshold(s) breached:</strong></p>
            <table style="width: 100%; border-collapse: collapse; margin-top: 10px;">
                <tr style="background: #f5f5f5;">
                    <th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">Metric</th>
                    <th style="padding: 8px; text-align: right; border-bottom: 2px solid #ddd;">Value</th>
                    <th style="padding: 8px; text-align: right; border-bottom: 2px solid #ddd;">Threshold</th>
                    <th style="padding: 8px; text-align: left; border-bottom: 2px solid #ddd;">Platform</th>
                </tr>
    """

    for alert in alerts:
        value = alert['current_value']
        threshold = alert['threshold']

        # Format as percentage if value is < 1
        if abs(value) < 1 and abs(threshold) < 1:
            val_str = f"{value:.2%}"
            thr_str = f"{alert['direction']} {threshold:.2%}"
        else:
            val_str = f"{value:,.2f}"
            thr_str = f"{alert['direction']} {threshold:,.2f}"

        html += f"""
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #eee;">
                        <strong>{alert['metric_name']}</strong>
                    </td>
                    <td style="padding: 8px; text-align: right; border-bottom: 1px solid #eee; color: #d32f2f;">
                        {val_str}
                    </td>
                    <td style="padding: 8px; text-align: right; border-bottom: 1px solid #eee;">
                        {thr_str}
                    </td>
                    <td style="padding: 8px; border-bottom: 1px solid #eee;">
                        {alert['platform']}
                    </td>
                </tr>
        """

    html += """
            </table>
            <p style="margin-top: 20px; color: #666; font-size: 12px;">
                This alert was generated by the DRR Dashboard automated monitoring system.
            </p>
        </div>
    </body>
    </html>
    """

    return html


def send_alert_email(alerts, week_ending_date):
    """Send alert email via Gmail API."""
    if not alerts:
        logger.info("No alerts to send")
        return False

    try:
        service = _get_gmail_service()
    except Exception as e:
        logger.error(f"Could not authenticate Gmail: {e}")
        return False

    html_body = _format_alert_email(alerts, week_ending_date)
    subject = f"DRR Dashboard Alert - {len(alerts)} threshold(s) breached - Week {week_ending_date}"

    # Plain text fallback
    plain_lines = [f"DRR Dashboard Alert - Week ending {week_ending_date}", ""]
    for a in alerts:
        plain_lines.append(f"  {a['metric_name']}: {a['current_value']} ({a['direction']} {a['threshold']})")
    plain_text = '\n'.join(plain_lines)

    success = True
    for recipient in ALERT_EMAILS:
        msg = MIMEMultipart('alternative')
        msg['To'] = recipient
        msg['Subject'] = subject

        msg.attach(MIMEText(plain_text, 'plain'))
        msg.attach(MIMEText(html_body, 'html'))

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        try:
            service.users().messages().send(
                userId='me', body={'raw': raw}
            ).execute()
            logger.info(f"Alert email sent to {recipient}")
        except Exception as e:
            logger.error(f"Failed to send alert email to {recipient}: {e}")
            success = False

    return success


def _format_weekly_summary(all_data, alerts, week_ending_date):
    """Format a comprehensive weekly summary email."""
    shopify = all_data.get('shopify', {})
    klaviyo = all_data.get('klaviyo', {})
    stripe_data = all_data.get('stripe', {})
    ghl = all_data.get('ghl', {})
    social = all_data.get('social', {})
    cross = all_data.get('cross_platform', {})

    total_rev = cross.get('total_revenue', shopify.get('gross_revenue', 0))

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">
        <div style="background: #1b5e20; color: white; padding: 20px; border-radius: 5px 5px 0 0;">
            <h1 style="margin: 0; font-size: 24px;">Dr. Ruth Roberts - Weekly Dashboard</h1>
            <p style="margin: 5px 0 0 0; opacity: 0.9;">Week ending: {week_ending_date}</p>
        </div>

        <div style="padding: 20px; border: 1px solid #ddd; border-top: none;">

        <!-- Revenue Overview -->
        <h2 style="color: #1b5e20; border-bottom: 2px solid #1b5e20; padding-bottom: 5px;">Revenue Overview</h2>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 8px;"><strong>Total Revenue</strong></td>
                <td style="padding: 8px; text-align: right; font-size: 18px; color: #1b5e20;">
                    <strong>${total_rev:,.2f}</strong>
                </td>
            </tr>
            <tr style="background: #f9f9f9;">
                <td style="padding: 8px;">E-Commerce</td>
                <td style="padding: 8px; text-align: right;">${shopify.get('ecommerce_revenue', 0):,.2f}</td>
            </tr>
            <tr>
                <td style="padding: 8px;">Coaching</td>
                <td style="padding: 8px; text-align: right;">${shopify.get('coaching_revenue', 0):,.2f}</td>
            </tr>
            <tr style="background: #f9f9f9;">
                <td style="padding: 8px;">Courses</td>
                <td style="padding: 8px; text-align: right;">${shopify.get('course_revenue', 0):,.2f}</td>
            </tr>
        </table>

        <!-- Key Metrics -->
        <h2 style="color: #1b5e20; border-bottom: 2px solid #1b5e20; padding-bottom: 5px; margin-top: 25px;">
            Key Metrics
        </h2>
        <table style="width: 100%; border-collapse: collapse;">
            <tr>
                <td style="padding: 6px;">Orders</td>
                <td style="padding: 6px; text-align: right;">{shopify.get('order_count', 0)}</td>
                <td style="padding: 6px;">AOV</td>
                <td style="padding: 6px; text-align: right;">${shopify.get('aov', 0):,.2f}</td>
            </tr>
            <tr style="background: #f9f9f9;">
                <td style="padding: 6px;">New Leads</td>
                <td style="padding: 6px; text-align: right;">{ghl.get('new_leads', 0)}</td>
                <td style="padding: 6px;">Close Rate</td>
                <td style="padding: 6px; text-align: right;">{ghl.get('close_rate_overall', 0):.1%}</td>
            </tr>
            <tr>
                <td style="padding: 6px;">Email Revenue</td>
                <td style="padding: 6px; text-align: right;">${klaviyo.get('email_attributed_revenue', 0):,.2f}</td>
                <td style="padding: 6px;">Open Rate</td>
                <td style="padding: 6px; text-align: right;">{klaviyo.get('open_rate', 0):.1%}</td>
            </tr>
            <tr style="background: #f9f9f9;">
                <td style="padding: 6px;">Payment Success</td>
                <td style="padding: 6px; text-align: right;">{stripe_data.get('payment_success_rate', 0):.1%}</td>
                <td style="padding: 6px;">MRR</td>
                <td style="padding: 6px; text-align: right;">${stripe_data.get('mrr', 0):,.2f}</td>
            </tr>
            <tr>
                <td style="padding: 6px;">Active Students</td>
                <td style="padding: 6px; text-align: right;">{ghl.get('active_students', 0)}</td>
                <td style="padding: 6px;">YT Subscribers</td>
                <td style="padding: 6px; text-align: right;">{social.get('yt_subscribers', 0):,}</td>
            </tr>
        </table>
    """

    # Alert section
    if alerts:
        html += f"""
        <h2 style="color: #d32f2f; border-bottom: 2px solid #d32f2f; padding-bottom: 5px; margin-top: 25px;">
            Alerts ({len(alerts)})
        </h2>
        <ul style="color: #d32f2f;">
        """
        for a in alerts:
            html += f"<li><strong>{a['metric_name']}</strong>: {a['current_value']} ({a['direction']} {a['threshold']})</li>\n"
        html += "</ul>"

    html += """
            <p style="margin-top: 25px; text-align: center;">
                <a href="https://drruthroberts-hphc.github.io/drr-dashboard/"
                   style="color: #2e7d32; font-weight: bold; font-size: 13px;">
                    View Live Dashboard
                </a>
            </p>
            <p style="margin-top: 10px; color: #999; font-size: 11px; text-align: center;">
                Generated by DRR Dashboard | Monday 6:00 AM Central
            </p>
        </div>
    </body>
    </html>
    """

    return html


def send_weekly_summary(all_data, alerts, week_ending_date):
    """Send the weekly summary email to all configured recipients."""
    try:
        service = _get_gmail_service()
    except Exception as e:
        logger.error(f"Could not authenticate Gmail: {e}")
        return False

    html_body = _format_weekly_summary(all_data, alerts, week_ending_date)
    subject = f"DRR Weekly Dashboard Summary - Week ending {week_ending_date}"

    success = True
    for recipient in ALERT_EMAILS:
        msg = MIMEMultipart('alternative')
        msg['To'] = recipient
        msg['Subject'] = subject

        msg.attach(MIMEText("Weekly dashboard summary - view in HTML-enabled email client.", 'plain'))
        msg.attach(MIMEText(html_body, 'html'))

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        try:
            service.users().messages().send(
                userId='me', body={'raw': raw}
            ).execute()
            logger.info(f"Weekly summary sent to {recipient}")
        except Exception as e:
            logger.error(f"Failed to send weekly summary to {recipient}: {e}")
            success = False

    return success


if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if '--auth' in sys.argv:
        # Manual OAuth flow - run this interactively to obtain gmail_token.json
        print("Starting Gmail OAuth flow...")
        print("NOTE: Your email must be added as a test user in Google Cloud Console first.")
        flow = InstalledAppFlow.from_client_secrets_file(
            GMAIL_CREDENTIALS, GMAIL_SCOPES
        )
        creds = flow.run_local_server(port=0)
        with open(GMAIL_TOKEN, 'w') as f:
            f.write(creds.to_json())
        print(f"Token saved to {GMAIL_TOKEN}")
    else:
        print("Usage: python alerts.py --auth  (to authorize Gmail)")
