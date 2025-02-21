"""
producer_song_attributes.py

Stream JSON data containing song attributes to a file and - if available - a Kafka topic.

Example JSON message
{
    "title": "Bohemian Rhapsody",
    "artist": "Queen",
    "genre": "Rock",
    "duration_seconds": 354,
    "release_year": 1975,
    "sentiment": 0.92
}

Environment variables are in utils/utils_config module.
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime

# import external modules
from kafka import KafkaProducer

# import from local modules
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

#####################################
# Stub Sentiment Analysis Function
#####################################


def assess_sentiment(title: str, artist: str) -> float:
    """
    Stub for sentiment analysis.
    Returns a random float between 0 and 1 for now.
    """
    return round(random.uniform(0, 1), 2)


#####################################
# Define Message Generator
#####################################


def generate_song_attributes():
    """
   Generate a stream of JSON messages containing song attributes.
    """
    TITLES = ["Bohemian Rhapsody", "Stairway to Heaven", "Hotel California", "Smells Like Teen Spirit", "Imagine"]
    ARTISTS = ["Queen", "Led Zeppelin", "Eagles", "Nirvana", "John Lennon"]
    GENRES = ["Rock", "Pop", "Jazz", "Classical", "Hip-Hop"]

    while True:
        title = random.choice(TITLES)
        artist = random.choice(ARTISTS)
        genre = random.choice(GENRES)
        
        duration_seconds = random.randint(180, 600)  # Random duration between 3 and 10 minutes
        release_year = random.randint(1960, 2020)
        sentiment = assess_sentiment(title, artist)

      # Create JSON message
        json_message = {
            "title": title,
            "artist": artist,
            "genre": genre,
            "duration_seconds": duration_seconds,
            "release_year": release_year,
            "sentiment": sentiment,
        }

        yield json_message



#####################################
# Define Main Function
#####################################


def main() -> None:

    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read required environment variables.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")

        logger.info("STEP 3. Build the path folders to the live data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    logger.info("STEP 4. Try to create a Kafka producer and topic.")
    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    if producer:
        try:
            create_kafka_topic(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    logger.info("STEP 5. Generate messages continuously.")
    try:
        for message in generate_messages():
            logger.info(message)

            with live_data_path.open("a") as f:
                f.write(json.dumps(message) + "\n")
                logger.info(f"STEP 4a Wrote message to file: {message}")

            # Send to Kafka if available
            if producer:
                producer.send(topic, value=message)
                logger.info(f"STEP 4b Sent message to Kafka topic '{topic}': {message}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("TRY/FINALLY: Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
