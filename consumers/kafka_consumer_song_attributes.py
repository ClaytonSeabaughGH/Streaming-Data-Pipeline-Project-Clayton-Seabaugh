"""
kafka_consumer_song_attributes.py

Consume JSON messages containing song attributes from a Kafka topic or file.
Store the data in a SQLite database and create a line graph of release years over time.
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import sys
from collections import defaultdict
from datetime import datetime

# import external modules
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
import sqlite3
import seaborn as sns

# import from local modules
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

#####################################
# Function to Process a Single Message
#####################################


def process_message(message: dict) -> dict:
    """
    Process and transform a single JSON message containing song attributes.

    Args:
        message (dict): The JSON message as a Python dictionary.

    Returns:
        dict: Processed message with additional fields if needed.
    """
    logger.info("Called process_message() with:")
    logger.info(f"   {message=}")
    processed_message = None
    try:
        processed_message = {
            "title": message.get("title"),
            "artist": message.get("artist"),
            "genre": message.get("genre"),
            "duration_seconds": int(message.get("duration_seconds", 0)),
            "release_year": int(message.get("release_year", 0)),
            "sentiment": float(message.get("sentiment", 0.0)),
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
    Initialize the SQLite database with a table to store song attributes.

    Args:
        sql_path (pathlib.Path): Path to the SQLite database file.
    """
    conn = sqlite3.connect(sql_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS songs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            artist TEXT,
            genre TEXT,
            duration_seconds INTEGER,
            release_year INTEGER,
            sentiment REAL
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
    conn = sqlite3.connect(sql_path)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO songs (
            title, artist, genre, duration_seconds, release_year, sentiment
        ) VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        message["title"],
        message["artist"],
        message["genre"],
        message["duration_seconds"],
        message["release_year"],
        message["sentiment"],
    ))
    conn.commit()
    conn.close()

#####################################
# Generate Combined graphs
#####################################

def generate_combined_graphs(sql_path: pathlib.Path):
    """
    Generate a single PNG image with three subplots:
    1. Release Years Over Time (Line Chart)
    2. Average Sentiment Over Time (Line Chart)
    3. Genre Distribution (Bar Chart)

    Args:
        sql_path (pathlib.Path): Path to the SQLite database file.
    """
    conn = sqlite3.connect(sql_path)
    cursor = conn.cursor()

    # Fetch Release Year Data
    cursor.execute('''
        SELECT release_year, COUNT(*) as count
        FROM songs
        GROUP BY release_year
        ORDER BY release_year
    ''')
    release_year_data = cursor.fetchall()

    # Fetch Sentiment Data
    cursor.execute('''
        SELECT release_year, AVG(sentiment) as avg_sentiment
        FROM songs
        GROUP BY release_year
        ORDER BY release_year
    ''')
    sentiment_data = cursor.fetchall()

    # Fetch Genre Data
    cursor.execute('''
        SELECT genre, COUNT(*) as count
        FROM songs
        GROUP BY genre
    ''')
    genre_data = cursor.fetchall()

    conn.close()

    # If no data, return early
    if not (release_year_data and sentiment_data and genre_data):
        logger.warning("No sufficient data available to generate combined graphs.")
        return

    # Extract data points
    years_release = [row[0] for row in release_year_data]
    counts_release = [row[1] for row in release_year_data]

    years_sentiment = [row[0] for row in sentiment_data]
    sentiments = [row[1] for row in sentiment_data]

    genres = [row[0] for row in genre_data]
    genre_counts = [row[1] for row in genre_data]

    # Set style
    sns.set_style("whitegrid")
    sns.set_palette("husl")

    # Create subplots (1 row, 3 columns)
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    for ax in axes:
        for spine in ax.spines.values():
            spine.set_edgecolor("#333333")
            spine.set_linewidth(1.5)

   
    # Release Year Graph
    axes[0].plot(years_release, counts_release, marker="o", linestyle="-", color="b")
    axes[0].set_title("Song Release Years Over Time", fontsize=13, fontweight="bold")
    axes[0].set_xlabel("Release Year", fontsize=11, fontstyle="italic")
    axes[0].set_ylabel("Number of Songs", fontsize=11, fontstyle="italic")
    axes[0].grid(True)

    # Sentiment Graph
    axes[1].plot(years_sentiment, sentiments, marker="o", linestyle="-", color="r")
    axes[1].fill_between(years_sentiment, sentiments, alpha=0.3, color="r")
    axes[1].axhline(y=0, linestyle="--", color="black", linewidth=1, alpha=0.7)
    axes[1].set_title("Average Song Sentiment Over Time", fontsize=13, fontweight="bold")
    axes[1].set_xlabel("Release Year", fontsize=11, fontstyle="italic")
    axes[1].set_ylabel("Average Sentiment", fontsize=11, fontstyle="italic")
    axes[1].grid(True)

    # Genre Distribution
    axes[2].bar(genres, genre_counts, color=sns.color_palette("coolwarm", len(genres)))
    axes[2].set_title("Song Genre Distribution", fontsize=13, fontweight="bold")
    axes[2].set_xlabel("Genre", fontsize=11, fontstyle="italic")
    axes[2].set_ylabel("Number of Songs", fontsize=11, fontstyle="italic")
    axes[2].tick_params(axis="x", rotation=45)

    # Adjust layout and save figure
    plt.tight_layout()
    fig.suptitle("Music Data Insights", fontsize=16, fontweight="bold")
    plt.subplots_adjust(top=0.88, wspace=0.35) 

    # Save figure
    plt.savefig("combined_graphs.png")
    plt.close()
    logger.info("Generated combined graph: combined_graphs.png")

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
                # Generate graph after inserting new data
                generate_combined_graphs(sql_path)
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