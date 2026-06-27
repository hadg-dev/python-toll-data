"""Kafka producer that simulates toll-road traffic events.

This script generates realistic toll passage events and publishes them to a Kafka
topic as JSON messages. Configuration is provided through environment variables
so it can be tuned for local development or production-like load testing.
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable

from kafka import KafkaProducer

LOGGER = logging.getLogger("toll.generator")

VEHICLE_TYPES = (
    "car",
    "car",
    "car",
    "car",
    "suv",
    "suv",
    "van",
    "van",
    "pickup",
    "truck",
    "truck",
    "bus",
    "motorcycle",
)
PAYMENT_METHODS = ("transponder", "transponder", "card", "card", "cash")
DIRECTIONS = ("N", "S", "E", "W")


@dataclass(frozen=True)
class TollEvent:
    """Represents a single toll passage event."""

    event_id: str
    event_timestamp: str
    plaza_id: int
    lane_id: int
    direction: str
    vehicle_id: int
    vehicle_type: str
    axle_count: int
    speed_kph: float
    payment_method: str
    toll_amount: float
    transponder_id: str | None

    def to_message(self) -> bytes:
        """Serialize the event into a Kafka message payload."""

        payload = json.dumps(asdict(self), separators=(",", ":"))
        return payload.encode("utf-8")


def setup_logging() -> None:
    """Configure structured logging for the generator."""

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def weighted_choice(rng: random.Random, items: Iterable[str]) -> str:
    """Pick a random element from an iterable."""

    items_list = list(items)
    return rng.choice(items_list)


def determine_axle_count(vehicle_type: str) -> int:
    """Estimate axle count based on vehicle type."""

    mapping = {
        "car": 2,
        "suv": 2,
        "van": 2,
        "pickup": 2,
        "motorcycle": 2,
        "truck": 4,
        "bus": 3,
    }
    return mapping.get(vehicle_type, 2)


def estimate_toll(vehicle_type: str, axle_count: int) -> float:
    """Estimate toll amount using vehicle type and axle count."""

    base = {
        "car": 2.75,
        "suv": 3.25,
        "van": 3.5,
        "pickup": 3.0,
        "motorcycle": 1.5,
        "truck": 6.5,
        "bus": 5.5,
    }.get(vehicle_type, 3.0)
    return round(base + (axle_count - 2) * 1.25, 2)


def generate_event(rng: random.Random) -> TollEvent:
    """Generate a single TollEvent with realistic attributes."""

    vehicle_type = weighted_choice(rng, VEHICLE_TYPES)
    axle_count = determine_axle_count(vehicle_type)
    payment_method = weighted_choice(rng, PAYMENT_METHODS)
    transponder_id = None
    if payment_method == "transponder":
        transponder_id = f"TX-{rng.randint(100000, 999999)}"

    speed_kph = round(rng.uniform(30.0, 120.0), 1)
    event_time = datetime.now(timezone.utc).isoformat()

    return TollEvent(
        event_id=f"EVT-{rng.randint(100000000, 999999999)}",
        event_timestamp=event_time,
        plaza_id=rng.randint(4000, 4020),
        lane_id=rng.randint(1, 12),
        direction=weighted_choice(rng, DIRECTIONS),
        vehicle_id=rng.randint(100000, 9999999),
        vehicle_type=vehicle_type,
        axle_count=axle_count,
        speed_kph=speed_kph,
        payment_method=payment_method,
        toll_amount=estimate_toll(vehicle_type, axle_count),
        transponder_id=transponder_id,
    )


def build_producer() -> KafkaProducer:
    """Create a Kafka producer with JSON serialization."""

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return KafkaProducer(bootstrap_servers=bootstrap_servers)


def main() -> None:
    """Entry point for the Kafka event generator."""

    setup_logging()
    topic = os.getenv("KAFKA_TOPIC", "toll_traffic")
    total_events = int(os.getenv("TOTAL_EVENTS", "500"))
    events_per_minute = int(os.getenv("EVENTS_PER_MINUTE", "120"))
    interval_seconds = max(0.1, 60 / max(events_per_minute, 1))
    rng = random.Random()

    producer = build_producer()
    LOGGER.info("Publishing %s events to topic '%s'", total_events, topic)

    for _ in range(total_events):
        event = generate_event(rng)
        producer.send(topic, value=event.to_message())
        LOGGER.info(
            "Toll event published",
            extra={"event_id": event.event_id, "plaza_id": event.plaza_id},
        )
        time.sleep(rng.uniform(interval_seconds * 0.6, interval_seconds * 1.4))

    producer.flush()
    producer.close()
    LOGGER.info("Generator finished publishing events")


if __name__ == "__main__":
    main()
