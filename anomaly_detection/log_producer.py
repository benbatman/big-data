import time
import json
import random
from datetime import datetime, timezone
import logging

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# A list of fake user IPs to select from
user_ips = [
    "192.168.1.101",
    "10.0.0.55",
    "172.16.0.12",
    "203.0.113.42",
    "198.51.100.7",
]

# A list of web pages on our fake site
web_pages = [
    "/",
    "/products/1",
    "/products/2",
    "/cart",
    "/checkout",
    "/about",
    "/admin",
]

# List of possible HTTP status codes, weighted to 200
http_status_codes = [
    200,
    200,
    200,
    200,
    200,
    200,
    200,
    301,
    404,
    404,
    500,
]


def create_log_message():
    """
    Generate a single, fake web log message
    """

    return {
        "timestamp_ms": int(time.time() * 1000),  # epoch ms
        "ip": random.choice(user_ips),
        "method": random.choice(["GET", "POST", "PUT", "DELETE"]),
        "url": random.choice(web_pages),
        "status_code": random.choice(http_status_codes),
        "response_time_ms": random.randint(50, 1500),  # ms
    }


def main():
    """
    Produce log message to kafka
    """
    # Producer connects to the Kafka container in Docker
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],  # connect to external Kafka broker
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Topic we're sending messages to
    kafka_topic = "web_logs_v2"
    logger.info(f"Producing messages to topic {kafka_topic}")

    while True:
        log_message = create_log_message()
        producer.send(kafka_topic, log_message)
        logger.info(f"Produced log message: {log_message}")
        if random.random() < 0.02:
            now_epoch_ms = int(time.time() * 1000)
            now_iso = datetime.now(timezone.utc).isoformat()
            logger.info(f"[PRODUCER NOW] epoch_ms={now_epoch_ms}, iso={now_iso}")
        time.sleep(random.uniform(0.1, 1.5))


if __name__ == "__main__":
    main()
