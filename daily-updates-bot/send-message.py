import zulip
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Use .zuliprc from current directory or home directory
config_path = ".zuliprc" if os.path.exists(".zuliprc") else os.path.expanduser("~/.zuliprc")
logger.info(f"Using config file: {config_path}")

client = zulip.Client(config_file=config_path)

STREAM_NAME = "core-team"     # channel/stream name
TOPIC = "daily-updates"     # topic inside the stream

MESSAGE_TEXT = """
👋 **Daily Update Time!**
Please share what you're doing today.
"""

def send_message():
    logger.info(f"Sending message to stream '{STREAM_NAME}', topic '{TOPIC}'")
    
    result = client.send_message({
        "type": "stream",
        "to": STREAM_NAME,
        "topic": TOPIC,
        "content": MESSAGE_TEXT,
    })
    
    if result.get("result") == "success":
        logger.info(f"Message sent successfully (id: {result.get('id')})")
    else:
        logger.error(f"Failed to send message: {result.get('msg', 'Unknown error')}")
    
    return result

if __name__ == "__main__":
    logger.info("Starting daily updates bot")
    send_message()
    logger.info("Bot execution completed")
