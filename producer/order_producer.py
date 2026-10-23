
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-orders"

CATEGORIES = ["Electronics", "Clothing", "Home", "Books"]


def generate_order():
    """Generate one e-commerce order event."""
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "category": random.choice(CATEGORIES),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10, 500), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    try:
        for number in range(100):
            order = generate_order()

            producer.send(
                KAFKA_TOPIC,
                key=order["order_id"],
                value=order,
            )

            print(f"Sent order {number + 1}: {order}")
            time.sleep(1)

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
