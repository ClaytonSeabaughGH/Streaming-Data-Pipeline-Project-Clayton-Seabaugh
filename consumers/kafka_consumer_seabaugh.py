"""
kafka_consumer_seabaugh.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Enhancements:
- Track and alert for categories based on keyword mentions using KEYWORD_CATEGORIES.
- Insert processed messages into SQLite with determined categories.
- Track sentiment categories (positive, negative, neutral) and store them in the database.
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys

# import external modules
from kafka import KafkaConsumer

# import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Define keyword to category mapping
KEYWORD_CATEGORIES = {
    "meme": "humor",
    "Python": "tech",
    "JavaScript": "tech",
    "recipe": "food",
    "travel": "travel",
    "movie": "entertainment",
    "game": "gaming",
}

#####################################
# Function to Categorize Sentiment
#####################################


def categorize_sentiment(sentiment_score: float) -> str:
    """
    Categorize the sentiment based on the sentiment score.

    Args:
        sentiment_score (float): The sentiment score.

    Returns:
        str: The sentiment category ('positive', 'negative', or 'neutral').
    """
    if sentiment_score > 0.1:
        return "positive"
    elif sentiment_score < -0.1:
        return "negative"
    else:
        return "neutral"


#####################################
# Function to Process a Single Message
#####################################


def process_message(message: dict) -> dict:
    """
    Process and transform a single JSON message.
    Determines category based on keyword_mentioned using KEYWORD_CATEGORIES.
    Converts message fields to appropriate data types.

    Args:
        message (dict): The JSON message as a Python dictionary.

    Returns:
        dict: Processed message with determined category and sentiment category.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")
    processed_message = None
    try:
        keyword_mentioned = message.get("keyword_mentioned")
        category = KEYWORD_CATEGORIES.get(keyword_mentioned, "unknown")
        sentiment_score = float(message.get("sentiment", 0.0))
        sentiment_category = categorize_sentiment(sentiment_score)

        # Log alerts based on keyword and category detection
        if keyword_mentioned:
            if category != "unknown":
                logger.info(f"Alert: Detected category '{category}' for keyword '{keyword_mentioned}'")
            else:
                logger.warning(f"Keyword '{keyword_mentioned}' not found in categories")
        else:
            logger.warning("No keyword mentioned in message")

        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": category,
            "sentiment": sentiment_score,
            "sentiment_category": sentiment_category,
            "keyword_mentioned": keyword_mentioned,
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message: {processed_message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")
    return processed_message


#####################################
# Initialize SQLite Database
#####################################


def init_db(sql_path: pathlib.Path):
    """
    Initialize the SQLite database with a table to store messages.

    Args:
        sql_path (pathlib.Path): Path to the SQLite database file.
    """
    import sqlite3

    conn = sqlite3.connect(sql_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            author TEXT,
            timestamp TEXT,
            category TEXT,
            sentiment REAL,
            sentiment_category TEXT,
            keyword_mentioned TEXT,
            message_length INTEGER
        )
    ''')
    conn.commit()
    conn.close()


#####################################
# Insert Processed Message into SQLite
#####################################


def insert_message(message: dict, sql_path: pathlib.Path):
    """
    Insert a processed message into the SQLite database.

    Args:
        message (dict): The processed message.
        sql_path (pathlib.Path): Path to the SQLite database file.
    """
    import sqlite3

    conn = sqlite3.connect(sql_path)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO messages (
            message, author, timestamp, category, sentiment, sentiment_category, keyword_mentioned, message_length
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        message["message"],
        message["author"],
        message["timestamp"],
        message["category"],
        message["sentiment"],
        message["sentiment_category"],
        message["keyword_mentioned"],
        message["message_length"],
    ))
    conn.commit()
    conn.close()


#####################################
# Consume Messages from Kafka Topic
#####################################


def consume_messages_from_kafka(
    topic: str,
    kafka_url: str,
    group: str,
    sql_path: pathlib.Path,
    interval_secs: int,
):
    """
    Consume new messages from Kafka topic and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - topic (str): Kafka topic to consume messages from.
    - kafka_url (str): Kafka broker address.
    - group (str): Consumer group ID for Kafka.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval between reads from the file.
    """
    logger.info("Called consume_messages_from_kafka() with:")
    logger.info(f"   {topic=}")
    logger.info(f"   {kafka_url=}")
    logger.info(f"   {group=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")

    logger.info("Step 1. Verify Kafka Services.")
    try:
        verify_services()
    except Exception as e:
        logger.error(f"ERROR: Kafka services verification failed: {e}")
        sys.exit(11)

    logger.info("Step 2. Create a Kafka consumer.")
    try:
        consumer: KafkaConsumer = create_kafka_consumer(
            topic,
            group,
            value_deserializer_provided=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as e:
        logger.error(f"ERROR: Could not create Kafka consumer: {e}")
        sys.exit(11)

    logger.info("Step 3. Verify topic exists.")
    if consumer is not None:
        try:
            is_topic_available(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.error(
                f"ERROR: Topic '{topic}' does not exist. Please run the Kafka producer. : {e}"
            )
            sys.exit(13)

    logger.info("Step 4. Process messages.")

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_message(processed_message, sql_path)
    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise


#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs: int = config.get_message_interval_seconds_as_int()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_kafka(
            topic, kafka_url, group_id, sqlite_path, interval_secs
        )
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()