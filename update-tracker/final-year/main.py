import os
import json
import datetime
import logging
import re
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


def today_label() -> str:
    return datetime.datetime.now(
        pytz.timezone(TIMEZONE)
    ).strftime("%-d %b")


def fetch_zulip_updates() -> dict:
    log.info("Connecting to Zulip at %s", ZULIP_SITE)

    client = zulip.Client(
        site=ZULIP_SITE,
        email=ZULIP_EMAIL,
        api_key=ZULIP_API_KEY,
    )

    tz = pytz.timezone(TIMEZONE)
    start_of_day = datetime.datetime.now(tz).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_timestamp = int(start_of_day.timestamp())

    log.info("Fetching messages from #%s > %s", CHANNEL_NAME, TOPIC_NAME)

    result = client.get_messages({
        "anchor": "newest",
        "num_before": 1000,
        "num_after": 0,
        "narrow": [
            {"operator": "stream", "operand": CHANNEL_NAME},
            {"operator": "topic", "operand": TOPIC_NAME},
        ],
    })

    # 🚨 HANDLE ZULIP ERRORS SAFELY
    if result.get("result") != "success":
        log.error("Zulip API error: %s", result.get("msg"))
        log.error("Full response: %s", result)
        return {}

    messages = result.get("messages", [])
    log.info("Fetched %d total messages from topic", len(messages))

    updates = {}
    for msg in messages:
        if msg.get("timestamp", 0) >= start_timestamp:
            user = msg.get("sender_full_name", "UNKNOWN")
            content = strip_html(msg.get("content", ""))
            updates[user] = content  # last message wins

    log.info(
        "Found %d users who posted today: %s",
        len(updates),
        list(updates.keys()),
    )

    return updates


def update_google_sheet(updates: dict) -> None:
    if not updates:
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
    today = today_label()

    # -------- HEADER --------
    if not all_values:
        sheet.update("A1", [["DATE"]])

    header = sheet.row_values(1)

    if not header or header[0].upper() != "DATE":
        sheet.insert_cols([[]], col=1)
        sheet.update_cell(1, 1, "DATE")
        header = ["DATE"] + header

    header_upper = [h.upper() for h in header]

    for user in updates:
        user_upper = user.upper()
        if user_upper not in header_upper:
            sheet.update_cell(1, len(header) + 1, user_upper)
            header.append(user_upper)
            header_upper.append(user_upper)

    # Bold header
    end_col_letter = chr(ord("A") + len(header) - 1)
    sheet.format(f"A1:{end_col_letter}1", {"textFormat": {"bold": True}})

    # -------- DATE ROW --------
    dates = sheet.col_values(1)

    if today in dates:
        row_idx = dates.index(today) + 1
    else:
        row_idx = len(dates) + 1
        sheet.update_cell(row_idx, 1, today)

    # -------- UPDATE CELLS --------
    for user, content in updates.items():
        col_idx = header_upper.index(user.upper()) + 1
        sheet.update_cell(row_idx, col_idx, content)
        log.info(
            "✓ %s (row %d, col %d)",
            user,
            row_idx,
            col_idx,
        )


def main():
    log.info("=== Update Tracker Started ===")

    updates = fetch_zulip_updates()
    if not updates:
        log.info("No messages found for today. Exiting.")
        return

    update_google_sheet(updates)
    log.info("=== Update Tracker Finished ===")


if __name__ == "__main__":
    main()
