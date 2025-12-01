import zulip
from datetime import datetime
import os

# Use .zuliprc from current directory or home directory
config_path = ".zuliprc" if os.path.exists(".zuliprc") else os.path.expanduser("~/.zuliprc")
client = zulip.Client(config_file=config_path)

STREAM_NAME = "core-team"     # channel/stream name
TOPIC = "daily-updates"     # topic inside the stream

MESSAGE_TEXT = """
👋 **Daily Update Time!**
Please share what you're doing today.
"""

def send_message():
    client.send_message({
        "type": "stream",
        "to": STREAM_NAME,
        "topic": TOPIC,
        "content": MESSAGE_TEXT,
    })

if __name__ == "__main__":
    send_message()
