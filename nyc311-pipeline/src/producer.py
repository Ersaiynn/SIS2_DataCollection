import json
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Kafka configuration
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "nyc311_requests"

# Path to collected API data
DATA_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "raw_data.json"
)


def send_to_kafka():
    # 1. Read raw data
    with open(DATA_PATH, "r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list) or len(records) < 200:
        raise ValueError("At least 200 records are required")

    print(f"Records loaded: {len(records)}")

    # 2. Create Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda record: json.dumps(
            record, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=3
    )

    published = 0
    failed = 0

    try:
        # 3. Send records
        for record in records:
            try:
                future = producer.send(
                    TOPIC_NAME,
                    value=record
                )

                # Wait for Kafka confirmation
                future.get(timeout=30)
                published += 1

            except KafkaError as error:
                failed += 1
                print(f"Failed to send message: {error}")

    finally:
        # 4. Close Producer
        producer.close()

    # 5. Display results
    print(f"\nTotal records: {len(records)}")
    print(f"Published messages: {published}")
    print(f"Failed messages: {failed}")

    if published < 200:
        raise RuntimeError("Less than 200 messages published")


if __name__ == "__main__":
    send_to_kafka()
