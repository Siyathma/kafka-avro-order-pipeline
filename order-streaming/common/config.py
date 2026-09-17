"""Shared configuration for the Kafka Avro order pipeline."""
import os

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders.DLQ")

# Resolved relative to this file, so it works regardless of where you run from.
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "order.avsc")