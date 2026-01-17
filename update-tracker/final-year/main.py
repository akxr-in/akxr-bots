import os
import json
import datetime
import logging
import re
from typing import Set, List, Dict

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
ROSTER_PATH = os.environ.get("ROSTER_PATH", "roster.json")
DM_MESSAGE = os.environ.get(
    "DM_MESSAGE",
    "Hey! You haven't posted your update in #{channel} > {topic} yet. Please share what you're working on today."
)
MENTION_MESSAGE = os.environ.get(
    "MENTION_MESSAGE",
    "Reminder: Please post your daily update."
)
# ----------------------------


def strip_html(text: str) -> str:
    """Remove HTML tags from Zulip messages."""
    return re.sub(r"<[^>]+>", "", text).strip()


def today_label() -> str:
    return datetime.datetime.now(
        pytz.timezone(TIMEZONE)
    ).strftime("%-d %b")


def today_date_str() -> str:
    """Return YYYY-MM-DD for today in configured timezone."""
    return datetime.datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")


def load_roster(path: str) -> dict:
    """Load batch/student roster from JSON file."""
    log.info("Loading roster from %s", path)
    with open(path) as f:
        roster = json.load(f)
    total_students = sum(len(b["students"]) for b in roster["batches"])
    log.info("Loaded %d batches with %d total students", len(roster["batches"]), total_students)
    return roster


def create_zulip_client() -> zulip.Client:
    """Create and return a Zulip client."""
    return zulip.Client(
        site=ZULIP_SITE,
        email=ZULIP_EMAIL,
        api_key=ZULIP_API_KEY,
    )


def get_users_who_posted_today(client: zulip.Client, channel: str, topic: str) -> Set[str]:
    """Fetch messages from today and return set of sender_email values."""
    tz = pytz.timezone(TIMEZONE)
    start_of_day = datetime.datetime.now(tz).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_timestamp = int(start_of_day.timestamp())

    log.info("Fetching messages from #%s > %s", channel, topic)

    result = client.get_messages({
        "anchor": "newest",
        "num_before": 1000,
        "num_after": 0,
        "narrow": [
            {"operator": "stream", "operand": channel},
            {"operator": "topic", "operand": topic},
        ],
    })

    if result.get("result") != "success":
        log.error("Zulip API error: %s", result.get("msg"))
        log.error("Full response: %s", result)
        return set()

    messages = result.get("messages", [])
    log.info("Fetched %d total messages from topic", len(messages))

    posted_users: Set[str] = set()
    for msg in messages:
        if msg.get("timestamp", 0) >= start_timestamp:
            username = msg.get("sender_email", "").lower()
            if username:
                posted_users.add(username)

    log.info("Found %d users who posted today: %s", len(posted_users), list(posted_users))
    return posted_users


def get_google_sheets_client():
    """Create and return authenticated gspread client."""
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDS"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def get_or_create_dm_state_sheet(gc) -> gspread.Worksheet:
    """Get or create the dm_state worksheet."""
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    try:
        return spreadsheet.worksheet("dm_state")
    except gspread.WorksheetNotFound:
        log.info("Creating dm_state worksheet")
        ws = spreadsheet.add_worksheet("dm_state", 1000, 4)
        ws.append_row(["DATE", "BATCH", "USERNAME", "TIMESTAMP"])
        return ws


def get_dmd_users_for_date(dm_sheet: gspread.Worksheet, batch: str, date: str) -> Set[str]:
    """Get usernames that were DM'd today for this batch from dm_state tab."""
    all_rows = dm_sheet.get_all_values()
    dmd_users: Set[str] = set()

    for row in all_rows[1:]:  # Skip header
        if len(row) >= 3 and row[0] == date and row[1] == batch:
            dmd_users.add(row[2].lower())

    return dmd_users


def record_dm_sent(dm_sheet: gspread.Worksheet, batch: str, date: str, username: str) -> None:
    """Append row to dm_state tab marking DM was sent."""
    timestamp = datetime.datetime.now(pytz.timezone(TIMEZONE)).isoformat()
    dm_sheet.append_row([date, batch, username.lower(), timestamp])
    log.info("Recorded DM sent to %s for batch %s", username, batch)


def send_dm(client: zulip.Client, username: str, channel: str, topic: str) -> bool:
    """Send private message to user. Returns True on success."""
    message = DM_MESSAGE.format(channel=channel, topic=topic)

    result = client.send_message({
        "type": "private",
        "to": [username],
        "content": message,
    })

    if result.get("result") != "success":
        log.error("DM failed for %s: %s", username, result.get("msg"))
        return False

    log.info("Sent DM to %s", username)
    return True


def send_channel_mention(
    client: zulip.Client,
    channel: str,
    topic: str,
    students: List[Dict[str, str]]
) -> bool:
    """Send @mention message in channel for multiple students."""
    if not students:
        return True

    mentions = " ".join(f'@**{s["display_name"]}**' for s in students)
    content = f"{mentions} {MENTION_MESSAGE}"

    result = client.send_message({
        "type": "stream",
        "to": channel,
        "topic": topic,
        "content": content,
    })

    if result.get("result") != "success":
        log.error("Channel mention failed: %s", result.get("msg"))
        return False

    log.info("Sent channel mention for %d students in #%s", len(students), channel)
    return True


def process_batch(
    batch: dict,
    zulip_client: zulip.Client,
    dm_sheet: gspread.Worksheet,
    batch_sheet: gspread.Worksheet,
    today: str
) -> None:
    """Process a single batch - check updates, send reminders, update sheet."""
    batch_name = batch["name"]
    channel = batch["channel"]
    students = batch["students"]

    log.info("Processing batch: %s (channel: %s, %d students)", batch_name, channel, len(students))

    # Get who posted today
    posted_users = get_users_who_posted_today(zulip_client, channel, TOPIC_NAME)

    # Fetch updates for this batch and update the batch sheet
    updates = fetch_batch_updates(zulip_client, channel, TOPIC_NAME)
    update_batch_sheet(batch_sheet, updates, batch_name)

    # Get who was already DM'd today
    already_dmd = get_dmd_users_for_date(dm_sheet, batch_name, today)

    # Categorize students
    to_dm: List[Dict[str, str]] = []
    to_mention: List[Dict[str, str]] = []
    posted_count = 0

    for student in students:
        username = student["username"].lower()
        if username in posted_users:
            posted_count += 1
        elif username in already_dmd:
            # Already DM'd but still no update -> public mention
            to_mention.append(student)
        else:
            # First offense today -> DM
            to_dm.append(student)

    log.info(
        "Batch %s: %d posted, %d to DM, %d to mention",
        batch_name, posted_count, len(to_dm), len(to_mention)
    )

    # Send DMs
    for student in to_dm:
        username = student["username"]
        if send_dm(zulip_client, username, channel, TOPIC_NAME):
            record_dm_sent(dm_sheet, batch_name, today, username)

    # Send public mention
    if to_mention:
        send_channel_mention(zulip_client, channel, TOPIC_NAME, to_mention)


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


def get_or_create_batch_sheet(gc, batch_name: str) -> gspread.Worksheet:
    """Get or create a worksheet for a specific batch."""
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    try:
        return spreadsheet.worksheet(batch_name)
    except gspread.WorksheetNotFound:
        log.info("Creating worksheet for batch: %s", batch_name)
        ws = spreadsheet.add_worksheet(batch_name, 1000, 50)
        ws.update("A1", [["DATE"]])
        ws.format("A1:A1", {"textFormat": {"bold": True}})
        return ws


def fetch_batch_updates(client: zulip.Client, channel: str, topic: str) -> dict:
    """Fetch today's updates from a specific batch channel."""
    tz = pytz.timezone(TIMEZONE)
    start_of_day = datetime.datetime.now(tz).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    start_timestamp = int(start_of_day.timestamp())

    log.info("Fetching messages from #%s > %s", channel, topic)

    result = client.get_messages({
        "anchor": "newest",
        "num_before": 1000,
        "num_after": 0,
        "narrow": [
            {"operator": "stream", "operand": channel},
            {"operator": "topic", "operand": topic},
        ],
    })

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

    log.info("Found %d users who posted today: %s", len(updates), list(updates.keys()))
    return updates


def update_batch_sheet(sheet: gspread.Worksheet, updates: dict, batch_name: str) -> None:
    """Update a batch-specific worksheet with today's updates."""
    if not updates:
        log.info("No updates to record for batch %s, skipping", batch_name)
        return

    log.info("Updating sheet for batch: %s", batch_name)

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
    if len(header) > 0:
        end_col_letter = chr(ord("A") + min(len(header) - 1, 25))
        if len(header) > 26:
            # Handle columns beyond Z (AA, AB, etc.)
            end_col_letter = "A" + chr(ord("A") + (len(header) - 27))
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
        log.info("✓ %s [%s] (row %d, col %d)", user, batch_name, row_idx, col_idx)


def update_google_sheet(updates: dict) -> None:
    """Legacy function - updates default sheet1."""
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
    log.info("=== Update Reminder Bot Started ===")

    # Load roster
    roster = load_roster(ROSTER_PATH)
    if not roster.get("batches"):
        log.warning("No batches found in roster. Exiting.")
        return

    # Initialize clients
    zulip_client = create_zulip_client()
    gc = get_google_sheets_client()
    dm_sheet = get_or_create_dm_state_sheet(gc)

    today = today_date_str()
    log.info("Processing for date: %s", today)

    # Process each batch with its own sheet
    for batch in roster["batches"]:
        try:
            batch_name = batch["name"]
            batch_sheet = get_or_create_batch_sheet(gc, batch_name)
            process_batch(batch, zulip_client, dm_sheet, batch_sheet, today)
        except Exception as e:
            log.exception("Failed processing batch %s: %s", batch.get("name", "unknown"), e)
            # Continue to next batch

    log.info("=== Update Reminder Bot Finished ===")


if __name__ == "__main__":
    main()
