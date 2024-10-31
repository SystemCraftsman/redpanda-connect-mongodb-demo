import os
import random
from datetime import datetime, timedelta
from enum import Enum

from confluent_kafka import Producer
import json

conf = {
    'bootstrap.servers': f"{os.environ.get('DEMO_BOOTSTRAP_SERVER')}:9092",
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': os.environ.get('DEMO_SASL_USERNAME'),
    'sasl.password': os.environ.get('DEMO_SASL_PASSWORD'),
}


class PlayerMove(str, Enum):
    JUMP = "Jump"
    LEFT = "Left"
    RIGHT = "Right"


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def random_user_id():
    return random.randint(0, 1000)


def random_datetime(min_year=2023, max_year=datetime.now().year):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()


def data():
    return {
        "player_id": random_user_id(),
        "move": random.choice(list(PlayerMove)),
        "timestamp": random_datetime().strftime("%Y-%m-%dT%H:%M:%SZ")
    }


if __name__ == "__main__":
    topic = 'player-moves'
    producer = Producer(**conf)

    for _ in range(100000):
        # print(json.dumps(data()).encode('utf-8'))
        producer.produce(topic, json.dumps(data()).encode('utf-8'), callback=delivery_report)

    producer.flush()
