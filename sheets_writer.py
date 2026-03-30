"""
Google Sheets Writer
====================
Appends weekly data rows to the DRR Dashboard Google Sheet.
Creates tabs if they don't exist, with proper column headers.
"""

import logging
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from config import GOOGLE_SHEET_ID, GOOGLE_SHEETS_CREDENTIALS, SHEET_TABS

logger = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]


def _get_client():
    """Authenticate and return a gspread client."""
    creds = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CREDENTIALS, scopes=SCOPES
    )
    return gspread.authorize(creds)


def _ensure_tab(spreadsheet, tab_name, columns):
    """Create a tab with headers if it doesn't exist."""
    existing = [ws.title for ws in spreadsheet.worksheets()]

    if tab_name in existing:
        return spreadsheet.worksheet(tab_name)

    logger.info(f"Creating new tab: {tab_name}")
    worksheet = spreadsheet.add_worksheet(
        title=tab_name, rows=500, cols=len(columns)
    )

    # Write header row
    worksheet.update('A1', [columns], value_input_option='RAW')

    # Bold header formatting
    worksheet.format('1:1', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.95},
    })

    return worksheet


def _check_duplicate(worksheet, week_ending_date):
    """Check if data for this week already exists."""
    col_a = worksheet.col_values(1)  # week_ending_date column
    date_str = str(week_ending_date)

    if date_str in col_a:
        row_idx = col_a.index(date_str) + 1
        logger.warning(f"Data for {date_str} already exists in row {row_idx}")
        return row_idx

    return None


def write_weekly_data(tab_name, data_dict, overwrite=False):
    """
    Write a row of weekly data to the specified tab.

    Args:
        tab_name: Name of the sheet tab (must be in SHEET_TABS)
        data_dict: Dict of metric values (keys must match SHEET_TABS columns)
        overwrite: If True, overwrite existing row for same week
    """
    if tab_name not in SHEET_TABS:
        raise ValueError(f"Unknown tab: {tab_name}. Valid tabs: {list(SHEET_TABS.keys())}")

    columns = SHEET_TABS[tab_name]

    client = _get_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    worksheet = _ensure_tab(spreadsheet, tab_name, columns)

    # Build row in column order
    row = []
    for col in columns:
        value = data_dict.get(col, '')
        row.append(value)

    # Check for duplicate
    week_date = data_dict.get('week_ending_date', '')
    existing_row = _check_duplicate(worksheet, week_date)

    if existing_row and not overwrite:
        logger.info(f"Skipping {tab_name} - data for {week_date} already exists (row {existing_row})")
        return False

    if existing_row and overwrite:
        # Update existing row
        cell_range = f"A{existing_row}"
        worksheet.update(cell_range, [row], value_input_option='USER_ENTERED')
        logger.info(f"Updated {tab_name} row {existing_row} for {week_date}")
    else:
        # Append new row
        worksheet.append_row(row, value_input_option='USER_ENTERED')
        logger.info(f"Appended new row to {tab_name} for {week_date}")

    return True


def write_all_weekly_data(shopify_data, klaviyo_data, ghl_data, stripe_data,
                          social_data, cross_platform_data, google_ads_data=None,
                          financial_data=None, overwrite=False):
    """
    Write all collector data to their respective tabs.

    Args:
        *_data: Dict from each collector's collect_weekly_data()
        cross_platform_data: Computed cross-platform aggregates
        google_ads_data: Google Ads collector data
        financial_data: Financial health metrics (derived from cross_platform)
        overwrite: If True, overwrite existing rows
    """
    results = {}

    tab_data_pairs = [
        ('Shopify_Weekly', shopify_data),
        ('Klaviyo_Weekly', klaviyo_data),
        ('GHL_Weekly', ghl_data),
        ('Stripe_Weekly', stripe_data),
        ('Social_Weekly', social_data),
        ('CrossPlatform_Weekly', cross_platform_data),
        ('GoogleAds_Weekly', google_ads_data),
        ('Financial_Weekly', financial_data),
    ]

    for tab_name, data in tab_data_pairs:
        if data:
            try:
                written = write_weekly_data(tab_name, data, overwrite=overwrite)
                results[tab_name] = 'written' if written else 'skipped'
            except Exception as e:
                logger.error(f"Error writing to {tab_name}: {e}")
                results[tab_name] = f'error: {e}'
        else:
            results[tab_name] = 'no data'
            logger.warning(f"No data provided for {tab_name}")

    return results


def write_alert(alert_data):
    """Write a single alert entry to the Alerts_Log tab."""
    try:
        write_weekly_data('Alerts_Log', alert_data, overwrite=False)
    except Exception as e:
        logger.error(f"Error writing alert: {e}")


def get_previous_week_data(tab_name, week_ending_date):
    """
    Get the previous week's data for WoW calculations.

    Returns:
        dict or None
    """
    if tab_name not in SHEET_TABS:
        return None

    columns = SHEET_TABS[tab_name]

    try:
        client = _get_client()
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

        existing = [ws.title for ws in spreadsheet.worksheets()]
        if tab_name not in existing:
            return None

        worksheet = spreadsheet.worksheet(tab_name)
        all_values = worksheet.get_all_values()

        if len(all_values) < 2:  # Only headers or empty
            return None

        # Find previous week's row
        prev_date = str(week_ending_date)
        headers = all_values[0]

        for row in reversed(all_values[1:]):
            if row[0] and row[0] < prev_date:
                return dict(zip(headers, row))

        return None

    except Exception as e:
        logger.error(f"Error fetching previous week data from {tab_name}: {e}")
        return None


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    # Test connection
    client = _get_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    print(f"Connected to: {spreadsheet.title}")
    print(f"Existing tabs: {[ws.title for ws in spreadsheet.worksheets()]}")
