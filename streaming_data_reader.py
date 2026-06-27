"""Kafka consumer that loads toll events into MySQL."""

from __future__ import annotations

import json
import logging
import os
import signal
from datetime import datetime
from typing import Any

from kafka import KafkaConsumer
import mysql.connector

LOGGER = logging.getLogger("toll.consumer")


def setup_logging() -> None:
    """Configure structured logging for the consumer."""

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def get_database_connection() -> mysql.connector.MySQLConnection:
    """Create a MySQL connection from environment variables."""

    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "tolldata"),
        user=os.getenv("DB_USER", "toll_user"),
        password=os.getenv("DB_PASSWORD", "changeme"),
    )


def build_consumer() -> KafkaConsumer:
    """Create a Kafka consumer configured for JSON payloads."""

    return KafkaConsumer(
        os.getenv("KAFKA_TOPIC", "toll_traffic"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        auto_offset_reset=os.getenv("KAFKA_OFFSET_RESET", "latest"),
        enable_auto_commit=True,
        group_id=os.getenv("KAFKA_CONSUMER_GROUP", "toll-ingestion"),
    )


def parse_event(raw_message: bytes) -> dict[str, Any]:
    """Parse a Kafka message payload into a dict."""

    return json.loads(raw_message.decode("utf-8"))


def normalize_timestamp(event_timestamp: str) -> str:
    """Normalize ISO timestamp to MySQL DATETIME format."""

    timestamp = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def insert_event(cursor: mysql.connector.cursor.MySQLCursor, event: dict[str, Any]) -> None:
    """Insert a parsed event into the target MySQL table."""

    sql = (
        "INSERT INTO livetolldata ("
        "event_id, event_timestamp, plaza_id, lane_id, direction, "
        "vehicle_id, vehicle_type, axle_count, speed_kph, payment_method, "
        "toll_amount, transponder_id"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    cursor.execute(
        sql,
        (
            event.get("event_id"),
            normalize_timestamp(event.get("event_timestamp")),
            event.get("plaza_id"),
            event.get("lane_id"),
            event.get("direction"),
            event.get("vehicle_id"),
            event.get("vehicle_type"),
            event.get("axle_count"),
            event.get("speed_kph"),
            event.get("payment_method"),
            event.get("toll_amount"),
            event.get("transponder_id"),
        ),
    )


def main() -> None:
    """Run the Kafka-to-MySQL ingestion loop."""

    setup_logging()
    LOGGER.info("Starting toll stream consumer")

    should_run = True

    def _handle_shutdown(_: int, __: object) -> None:
        nonlocal should_run
        should_run = False
        LOGGER.info("Shutdown signal received")

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    try:
        connection = get_database_connection()
    except mysql.connector.Error as exc:
        LOGGER.error("Could not connect to database: %s", exc)
        return

    consumer = build_consumer()
    cursor = connection.cursor()

    try:
        for message in consumer:
            if not should_run:
                break
            event = parse_event(message.value)
            insert_event(cursor, event)
            connection.commit()
            LOGGER.info(
                "Inserted toll event",
                extra={"event_id": event.get("event_id"), "plaza_id": event.get("plaza_id")},
            )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Unhandled error while consuming events: %s", exc)
    finally:
        cursor.close()
        connection.close()
        consumer.close()
        LOGGER.info("Consumer stopped")


if __name__ == "__main__":
    main()
