import os
import json
import datetime
import pytz
import zulip
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# ---------- CONFIG ----------
ZULIP_SITE = os.environ["ZULIP_SITE"]
ZULIP_EMAIL = os.environ["ZULIP_EMAIL"]
ZULIP_API_KEY = os.environ["ZULIP_API_KEY"]

CHANNEL_NAME = os.environ["ZULIP_CHANNEL"]
TOPIC_NAME = os.environ["ZULIP_TOPIC"]
SPREADSHEET_ID = os.environ["GSHEET_ID"]

TIMEZONE = "Asia/Kolkata"
STATUS_TEXT = "did something"
# ----------------------------


def today_label():
    return datetime.datetime.now(
        pytz.timezone(TIMEZONE)
    ).strftime("%-d %b")


def fetch_zulip_users():
    client = zulip.Client(
        site=ZULIP_SITE,
        email=ZULIP_EMAIL,
        api_key=ZULIP_API_KEY,
    )

    tz = pytz.timezone(TIMEZONE)
    start = datetime.datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)

    result = client.get_messages({
        "anchor": int(start.timestamp()),
        "num_before": 0,
        "num_after": 1000,
        "narrow": [
            {"operator": "stream", "operand": CHANNEL_NAME},
            {"operator": "topic", "operand": TOPIC_NAME},
        ],
    })

    users = set()
    for msg in result["messages"]:
        users.add(msg["sender_full_name"])

    return list(users)


def update_google_sheet(users):
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )

    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).sheet1

    all_values = sheet.get_all_values()
    today = today_label()

    # -------- HEADER (ROW 1) --------
    if not all_values:
        sheet.update("A1", [[""]])

    header = sheet.row_values(1)

    for user in users:
        if user not in header:
            sheet.update_cell(1, len(header) + 1, user)
            header.append(user)

    # -------- DATE ROW --------
    dates = sheet.col_values(1)

    if today in dates:
        row_idx = dates.index(today) + 1
    else:
        row_idx = len(dates) + 1
        sheet.update_cell(row_idx, 1, today)

    # -------- UPDATE CELLS --------
    for user in users:
        col_idx = header.index(user) + 1
        sheet.update_cell(row_idx, col_idx, STATUS_TEXT)


def main():
    users = fetch_zulip_users()
    update_google_sheet(users)


if __name__ == "__main__":
    main()
