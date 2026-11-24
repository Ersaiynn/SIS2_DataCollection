# NYC 311 Data Streaming Pipeline

## Project Overview

This project collects real NYC 311 service requests from a public API and streams them through Apache Kafka.

## Data Source

API: https://data.cityofnewyork.us/resource/erm2-nwe9.json

Dataset: 500 records collected from five selected days in October 2026.

## Requirements

- Python
- Docker Desktop
- Apache Kafka
- requests
- kafka-python

## Installation

```bash
pip install -r requirements.txt
```

## Running the Pipeline

**1. Start Kafka Broker**

```bash
docker compose up -d
```

**2. Create Kafka Topic**

```bash
docker exec nyc311-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic nyc311_requests --partitions 1 --replication-factor 1
```

**3. Collect API Data**

```bash
python src/api_client.py
```

**4. Run Kafka Producer**

```bash
python src/producer.py
```

**5. Verify Kafka Messages**

```bash
docker exec nyc311-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic nyc311_requests --from-beginning --max-messages 3
```

## Expected Results

- 500 collected API records
- 500 successfully published Kafka messages
- JSON messages available in the Kafka Topic

Kafka Topic: `nyc311_requests`

Kafka Bootstrap Server: `localhost:9092`

*Consumer, validation, cleaning, database storage and analysis will be integrated by the other group members.*