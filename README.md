# Kafka Consumer: Seabaugh Edition (buzzline-05-seabaugh)

Welcome to **Kafka Consumer: Seabaugh Edition**, where real-time data processing meets fun and functionality! 🚀

## 📌 Overview
This Kafka consumer is designed to consume JSON messages from a live data stream, process them, and insert the results into an SQLite database. But wait—there’s more! It also tracks sentiment and categorizes messages based on keyword mentions. Think of it as your personal data butler, sorting and analyzing messages with precision and a touch of flair. 🎩✨

## 🔥 Features
- **Live Kafka Consumer**: Fetches messages in real time from a Kafka topic.
- **Keyword-Based Categorization**: Detects predefined keywords and assigns categories like `tech`, `gaming`, `food`, and more.
- **Sentiment Analysis**: Determines whether a message has `positive`, `negative`, or `neutral` sentiment.
- **SQLite Integration**: Stores processed messages for future analysis.
- **Smart Alerting**: Notifies when a recognized keyword is detected in a message.
- **Robust Logging**: Tracks every step, ensuring transparency and easy debugging.

## 🛠️ How It Works
1. **Message Consumption**: The script connects to Kafka and consumes messages from a specified topic.
2. **Processing Magic**: Each message is checked for keywords, categorized, and assigned a sentiment score.
3. **Data Storage**: Processed messages are inserted into an SQLite database for safekeeping.
4. **Alerts & Logs**: If a keyword is detected, the consumer logs an alert. If something goes wrong, it logs that too!

## 🏗️ Setup & Execution
### 1️⃣ Install Dependencies
Make sure you have the necessary Python packages installed. Run:
```bash
pip install -r requirements.txt
```

### 2️⃣ Configure Environment Variables
Set up your `.env` file or use the `utils_config` module to configure:
- Kafka topic
- Kafka broker address
- Consumer group ID
- Database file path
- Message polling interval

### 3️⃣ Start the Consumer
Run the script and watch the magic happen:
```bash
python kafka_consumer_seabaugh.py
```

## 🧠 Fun Fact
This consumer doesn’t just process messages—it *appreciates* them. If you send a message with `meme`, it knows you’re in the mood for humor. If it sees `Python`, it knows you're a tech enthusiast. Smart, right? 🤓

## 🎯 Keyword Categories
| Keyword     | Category       |
|------------|---------------|
| meme       | humor         |
| Python     | tech          |
| JavaScript | tech          |
| recipe     | food          |
| travel     | travel        |
| movie      | entertainment |
| game       | gaming        |

## 📊 Sentiment Analysis
- **Positive** (score > 0.1) → 😃
- **Negative** (score < -0.1) → 😠
- **Neutral** (otherwise) → 😐

## 🚀 Future Enhancements
- **Real-time dashboard for visualization** 📊
- **Integration with external databases like PostgreSQL** 🛢️
- **Machine learning-powered sentiment analysis** 🤖

### 📢 Final Words
Whether you're analyzing trends or just keeping an eye on what’s buzzing, **Kafka Consumer: Seabaugh Edition** is here to make data processing efficient and fun. Happy consuming! 🎉

