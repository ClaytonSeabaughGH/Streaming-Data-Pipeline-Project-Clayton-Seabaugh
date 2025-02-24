# Kafka Producer & Consumer: Seabaugh Edition (buzzline-06-seabaugh)

Welcome to **Kafka Producer & Consumer: Seabaugh Edition**, where real-time data streaming meets powerful analytics! 🚀

## 📌 Overview
This system consists of both a **Kafka Producer** and a **Kafka Consumer** to handle real-time streaming of JSON messages related to song attributes. The producer sends song data to a Kafka topic, while the consumer processes the messages, performs sentiment analysis, and stores them in an SQLite database. It also generates insightful graphs for visualization.

## 🔥 Features
### Producer Features
- **Real-Time Data Streaming**: Sends song-related JSON messages to a Kafka topic.
- **Dynamic Message Generation**: Simulates song metadata including title, artist, genre, duration, release year, and sentiment.
- **Customizable Configuration**: Modify parameters such as message frequency and Kafka topic via environment variables.

### Consumer Features
- **Live Kafka Consumption**: Fetches messages in real-time from a Kafka topic.
- **Sentiment Analysis**: Analyzes song attributes and assigns a sentiment score.
- **SQLite Integration**: Stores processed messages for future analysis.
- **Graph Generation**: Creates visual representations of release trends, sentiment over time, and genre distribution.
- **Logging & Alerts**: Tracks operations and ensures transparent debugging.

## 🛠️ How It Works
1. **Kafka Producer** generates song data and sends it to a Kafka topic.
2. **Kafka Consumer** retrieves messages from the topic, processes them, and performs analysis.
3. **Database Storage**: Processed messages are inserted into an SQLite database.
4. **Graph Generation**: Consumer generates graphs showcasing trends in music data.

## 🏗️ Setup & Execution
### 1️⃣ Install Dependencies
Ensure you have all required Python packages installed:
```bash
pip install -r requirements.txt
```

### 2️⃣ Configure Environment Variables
Use a `.env` file or `utils_config` module to set up:
- Kafka topic
- Kafka broker address
- Producer message interval
- Consumer group ID
- SQLite database file path

### 3️⃣ Start the Kafka Producer
Run the producer to send song data:
```bash
python kafka_producer_seabaugh.py
```

### 4️⃣ Start the Kafka Consumer
Run the consumer to process and store messages:
```bash
python kafka_consumer_seabaugh.py
```

## 📊 Data Visualization
Once messages are processed, the consumer generates graphs:
- **Song Release Trends**: Line graph of release years over time.
- **Sentiment Analysis Over Time**: Tracks how song sentiment evolves by release year.
- **Genre Distribution**: Bar chart showing the frequency of different genres.

Graphs are saved as `combined_graphs.png` in the working directory.

## 🚀 Future Enhancements
- **Real-time dashboard for visualization** 📊
- **Integration with external databases like PostgreSQL** 🛢️
- **Machine learning-powered sentiment analysis** 🤖

### 📢 Final Words
With **Kafka Producer & Consumer: Seabaugh Edition**, you can efficiently stream, process, and analyze music data in real-time. Whether you're tracking trends or diving deep into sentiment analysis, this system has you covered. Happy streaming! 🎶

