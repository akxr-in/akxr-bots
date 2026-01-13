"""
Temporary script to backfill all historical messages from Zulip to Google Sheet.
Run once to populate all past updates.
"""

import os
import json
import datetime
import logging
import re
from collections import defaultdict

import pytz
import zulip
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------- CONFIG ----------
ZULIP_SITE = os.environ["ZULIP_SITE"]
ZULIP_EMAIL = os.environ["ZULIP_EMAIL"]
ZULIP_API_KEY = os.environ["ZULIP_API_KEY"]

CHANNEL_NAME = os.environ["ZULIP_CHANNEL"]
TOPIC_NAME = os.environ["ZULIP_TOPIC"]
SPREADSHEET_ID = os.environ["GSHEET_ID"]

TIMEZONE = "Asia/Kolkata"
# ----------------------------


def strip_html(text: str) -> str:
    """Remove HTML tags from Zulip messages."""
    return re.sub(r"<[^>]+>", "", text).strip()


def date_label(timestamp: int) -> str:
    """Convert timestamp to date label like '13 Jan'."""
    tz = pytz.timezone(TIMEZONE)
    dt = datetime.datetime.fromtimestamp(timestamp, tz=tz)
    return dt.strftime("%-d %b")


def fetch_all_zulip_messages() -> dict:
    """Fetch ALL messages and group by date -> user -> content."""
    log.info("Connecting to Zulip at %s", ZULIP_SITE)

    client = zulip.Client(
        site=ZULIP_SITE,
        email=ZULIP_EMAIL,
        api_key=ZULIP_API_KEY,
    )

    log.info("Fetching ALL messages from #%s > %s", CHANNEL_NAME, TOPIC_NAME)

    all_messages = []
    anchor = "newest"

    # Paginate through all messages
    while True:
        result = client.get_messages({
            "anchor": anchor,
            "num_before": 1000,
            "num_after": 0,
            "narrow": [
                {"operator": "stream", "operand": CHANNEL_NAME},
                {"operator": "topic", "operand": TOPIC_NAME},
            ],
        })

        if result.get("result") != "success":
            log.error("Zulip API error: %s", result.get("msg"))
            log.error("Full response: %s", result)
            break

        messages = result.get("messages", [])
        if not messages:
            break

        all_messages.extend(messages)
        log.info("Fetched %d messages so far...", len(all_messages))

        # Get oldest message ID for next pagination
        oldest_id = min(msg["id"] for msg in messages)
        if anchor == oldest_id:
            break
        anchor = oldest_id

        # If we got less than requested, we've reached the beginning
        if len(messages) < 1000:
            break

    log.info("Total messages fetched: %d", len(all_messages))

    # Group messages by date -> user -> content (last message per user per day wins)
    updates_by_date = defaultdict(dict)
    for msg in all_messages:
        date = date_label(msg.get("timestamp", 0))
        user = msg.get("sender_full_name", "UNKNOWN")
        content = strip_html(msg.get("content", ""))
        updates_by_date[date][user] = content

    log.info("Found updates for %d dates", len(updates_by_date))
    return dict(updates_by_date)


def update_google_sheet(updates_by_date: dict) -> None:
    if not updates_by_date:
        log.info("No updates to record, skipping sheet update")
        return

    log.info("Connecting to Google Sheets")

    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )

    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).sheet1

    all_values = sheet.get_all_values()

    # -------- HEADER --------
    if not all_values:
        sheet.update("A1", [["DATE"]])

    header = sheet.row_values(1)

    if not header or header[0].upper() != "DATE":
        sheet.insert_cols([[]], col=1)
        sheet.update_cell(1, 1, "DATE")
        header = ["DATE"] + header

    header_upper = [h.upper() for h in header]

    # Collect all users from all dates
    all_users = set()
    for updates in updates_by_date.values():
        all_users.update(updates.keys())

    # Add missing users to header
    for user in sorted(all_users):
        user_upper = user.upper()
        if user_upper not in header_upper:
            sheet.update_cell(1, len(header) + 1, user_upper)
            header.append(user_upper)
            header_upper.append(user_upper)
            log.info("Added new user to header: %s", user)

    # Bold header
    end_col_letter = chr(ord("A") + min(len(header) - 1, 25))
    if len(header) > 26:
        end_col_letter = "A" + chr(ord("A") + (len(header) - 1 - 26))
    sheet.format(f"A1:{end_col_letter}1", {"textFormat": {"bold": True}})

    # -------- PROCESS EACH DATE --------
    dates = sheet.col_values(1)

    # Sort dates chronologically
    tz = pytz.timezone(TIMEZONE)

    def parse_date(d):
        try:
            # Parse "13 Jan" format
            return datetime.datetime.strptime(f"{d} 2025", "%d %b %Y")
        except:
            return datetime.datetime.min

    sorted_dates = sorted(updates_by_date.keys(), key=parse_date)

    for date in sorted_dates:
        updates = updates_by_date[date]

        if date in dates:
            row_idx = dates.index(date) + 1
        else:
            row_idx = len(dates) + 1
            sheet.update_cell(row_idx, 1, date)
            dates.append(date)

        # Update cells for this date
        for user, content in updates.items():
            col_idx = header_upper.index(user.upper()) + 1
            sheet.update_cell(row_idx, col_idx, content)

        log.info("✓ Updated %s with %d entries", date, len(updates))


def main():
    log.info("=== Backfill Script Started ===")

    updates_by_date = fetch_all_zulip_messages()
    if not updates_by_date:
        log.info("No messages found. Exiting.")
        return

    update_google_sheet(updates_by_date)
    log.info("=== Backfill Complete ===")


if __name__ == "__main__":
    main()
