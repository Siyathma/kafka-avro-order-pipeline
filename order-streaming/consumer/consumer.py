"""Order consumer: deserializes Avro order messages, maintains a running
average of prices, retries transient failures with backoff, and routes
permanently failed messages to a Dead Letter Queue."""
from __future__ import annotations

import json
import logging
import random
import time

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from common import config
from consumer.aggregator import RunningAverage
from consumer.errors import PermanentError, TransientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("consumer")

CONSUMER_GROUP = "orders-consumer-group"
MAX_RETRIES = 3
RETRY_BASE_DELAY = 0.5  # seconds; doubles each attempt (exponential backoff)

# Failure-injection knobs so we can demonstrate retry and DLQ live.
TRANSIENT_FAILURE_RATE = 0.15  # 15% of messages hit a simulated transient glitch


def process_order(order: dict) -> None:
    """Business logic for a single order.

    Raises:
        PermanentError: the order is invalid and will never process.
        TransientError: a temporary glitch; retrying may succeed.
    """
    # --- Permanent validation: bad data can never become good on retry ---
    price = order.get("price")
    if price is None or price < 0:
        raise PermanentError(f"Invalid price: {price!r}")

    # --- Simulated transient failure (e.g. a flaky downstream call) ---
    if random.random() < TRANSIENT_FAILURE_RATE:
        raise TransientError("Simulated downstream timeout")

    # Success: nothing else to do here; aggregation happens in the caller.


def process_with_retry(order: dict) -> None:
    """Attempt processing, retrying transient failures with exponential backoff.

    Permanent failures propagate immediately (no retry). If retries are
    exhausted, the last TransientError is re-raised so the caller can DLQ it.
    """
    attempt = 0
    while True:
        try:
            process_order(order)
            return
        except PermanentError:
            raise  # do not retry; caller routes to DLQ
        except TransientError as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                log.warning("Retries exhausted after %d attempts", MAX_RETRIES)
                raise
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
            log.info("Transient failure (attempt %d/%d): %s — retrying in %.1fs",
                     attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)


def send_to_dlq(producer: Producer, string_serializer: StringSerializer,
                msg, error: Exception) -> None:
    """Publish a failed message to the DLQ with diagnostic metadata.

    The DLQ payload is JSON (not Avro): it must survive even messages that
    failed *because* they couldn't be deserialized, and it carries context
    a raw Avro record can't.
    """
    dlq_record = {
        "error": str(error),
        "error_type": type(error).__name__,
        "original_topic": msg.topic(),
        "original_partition": msg.partition(),
        "original_offset": msg.offset(),
        "key": msg.key().decode("utf-8") if msg.key() else None,
        # Raw value as hex — the original bytes, for later inspection/replay.
        "value_hex": msg.value().hex() if msg.value() else None,
    }
    producer.produce(
        topic=config.DLQ_TOPIC,
        key=string_serializer(dlq_record["key"] or "unknown"),
        value=json.dumps(dlq_record).encode("utf-8"),
    )
    producer.flush()
    log.error("Routed to DLQ: %s (%s)", dlq_record["error_type"], dlq_record["error"])


def main() -> None:
    sr_client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(sr_client)
    string_serializer = StringSerializer("utf_8")

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",   # start from the beginning on first run
        "enable.auto.commit": False,       # we commit manually, only after handling
    })
    consumer.subscribe([config.ORDERS_TOPIC])

    # A dedicated producer for DLQ output.
    dlq_producer = Producer({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

    aggregator = RunningAverage()

    log.info("Consuming from '%s'. Press Ctrl+C to stop.", config.ORDERS_TOPIC)
    try:
        while True:
            msg = consumer.poll(1.0)  # wait up to 1s for a message
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # --- Deserialize; a failure here is permanent (poison message) ---
            try:
                order = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as e:  # noqa: BLE001 — any deser error is permanent
                send_to_dlq(dlq_producer, string_serializer, msg,
                            PermanentError(f"Deserialization failed: {e}"))
                consumer.commit(msg)  # handled (parked in DLQ) — advance offset
                continue

            # --- Process with retry; DLQ on permanent or exhausted retries ---
            try:
                process_with_retry(order)
            except (PermanentError, TransientError) as e:
                send_to_dlq(dlq_producer, string_serializer, msg, e)
                consumer.commit(msg)
                continue

            # --- Success: update aggregation and commit ---
            current_avg = aggregator.add(float(order["price"]))
            log.info(
                "Processed %s | price=%.2f | running avg=%.2f over %d orders",
                order["orderId"], order["price"], current_avg, aggregator.count,
            )
            consumer.commit(msg)

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        consumer.close()
        dlq_producer.flush()
        log.info("Consumer closed. Final average: %.2f over %d orders.",
                 aggregator.mean, aggregator.count)


if __name__ == "__main__":
    main()