"""Order producer: generates random orders, serializes them with Avro,
and publishes them to the 'orders' Kafka topic."""
from __future__ import annotations

import logging
import random
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from common import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("producer")

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def load_schema(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def order_to_dict(order: dict, ctx: SerializationContext) -> dict:
    """Hook the AvroSerializer calls to convert an object into a dict.
    Our order is already a plain dict, so we return it unchanged."""
    return order


def generate_order() -> dict:
    return {
        "orderId": str(random.randint(1000, 9999)),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def delivery_report(err, msg) -> None:
    """Called once per message to report the final delivery outcome."""
    if err is not None:
        log.error("Delivery FAILED (key=%s): %s", msg.key(), err)
    else:
        log.info(
            "Delivered to %s [p%s] @ offset %s",
            msg.topic(), msg.partition(), msg.offset(),
        )


def main() -> None:
    schema_str = load_schema(config.SCHEMA_PATH)

    sr_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr_client, schema_str, to_dict=order_to_dict)
    string_serializer = StringSerializer("utf_8")

    producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

    log.info("Producing to '%s'. Press Ctrl+C to stop.", config.ORDERS_TOPIC)
    try:
        while True:
            order = generate_order()
            producer.produce(
                topic=config.ORDERS_TOPIC,
                key=string_serializer(order["orderId"]),
                value=avro_serializer(
                    order,
                    SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE),
                ),
                on_delivery=delivery_report,
            )
            # Serve queued delivery callbacks without blocking.
            producer.poll(0)
            log.info("Queued: %s", order)
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        # Block until every buffered message is delivered or fails.
        producer.flush()
        log.info("All messages flushed. Bye.")


if __name__ == "__main__":
    main()