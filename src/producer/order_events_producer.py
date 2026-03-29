import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from google.cloud import pubsub_v1


STORES = {
    "store_101": "NORTH",
    "store_102": "SOUTH",
    "store_103": "EAST",
    "store_104": "WEST",
}

CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso8601(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def random_items(rng: random.Random) -> list[dict[str, Any]]:
    item_count = rng.randint(1, 4)
    items = []

    for _ in range(item_count):
        category = rng.choice(CATEGORIES)
        quantity = rng.randint(1, 3)
        unit_price = round(rng.uniform(20.0, 300.0), 2)
        items.append(
            {
                "sku": f"{category[:3].upper()}-{rng.randint(1000, 9999)}",
                "category": category,
                "qty": quantity,
                "unit_price": unit_price,
                "line_amount": round(quantity * unit_price, 2),
            }
        )

    return items


def base_event(order_id: str, customer_id: str, store_id: str, event_type: str, event_ts: datetime) -> dict[str, Any]:
    region = STORES[store_id]
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "event_type": event_type,
        "event_ts": to_iso8601(event_ts),
        "store_id": store_id,
        "region": region,
        "currency": "USD",
        "ingest_source": "python-simulator",
    }


def build_order_bundle(rng: random.Random) -> list[dict[str, Any]]:
    order_id = f"ORD-{uuid.uuid4().hex[:10].upper()}"
    customer_id = f"CUST-{rng.randint(100000, 999999)}"
    store_id = rng.choice(list(STORES.keys()))
    created_ts = utc_now() - timedelta(seconds=rng.randint(0, 180))

    items = random_items(rng)
    order_amount = round(sum(item["line_amount"] for item in items), 2)

    created_event = base_event(order_id, customer_id, store_id, "ORDER_CREATED", created_ts)
    created_event["items"] = items
    created_event["order_amount"] = order_amount
    created_event["payment_status"] = "PENDING"
    created_event["shipment_status"] = "PENDING"

    payment_ts = created_ts + timedelta(minutes=rng.randint(1, 5))
    payment_event = base_event(order_id, customer_id, store_id, "PAYMENT_CAPTURED", payment_ts)
    payment_event["items"] = items
    payment_event["order_amount"] = order_amount
    payment_event["payment_status"] = "CAPTURED"
    payment_event["shipment_status"] = "PENDING"

    dispatch_ts = payment_ts + timedelta(minutes=rng.randint(15, 180))
    dispatch_event = base_event(order_id, customer_id, store_id, "SHIPMENT_DISPATCHED", dispatch_ts)
    dispatch_event["items"] = items
    dispatch_event["order_amount"] = order_amount
    dispatch_event["payment_status"] = "CAPTURED"
    dispatch_event["shipment_status"] = "DISPATCHED"

    delivery_ts = dispatch_ts + timedelta(hours=rng.randint(4, 60))
    delivery_event = base_event(order_id, customer_id, store_id, "DELIVERY_COMPLETED", delivery_ts)
    delivery_event["items"] = items
    delivery_event["order_amount"] = order_amount
    delivery_event["payment_status"] = "CAPTURED"
    delivery_event["shipment_status"] = "DELIVERED"

    return [created_event, payment_event, dispatch_event, delivery_event]


def publish_events(project_id: str, topic_id: str, event_count: int, sleep_seconds: float, seed: int) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    rng = random.Random(seed)

    published = 0
    while published < event_count:
        for event in build_order_bundle(rng):
            if published >= event_count:
                break

            future = publisher.publish(
                topic_path,
                json.dumps(event).encode("utf-8"),
                event_type=event["event_type"],
                order_id=event["order_id"],
            )
            future.result(timeout=30)
            published += 1
            print(f"Published {published}/{event_count}: {event['event_type']} for {event['order_id']}")

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish simulated retail events to Pub/Sub.")
    parser.add_argument("--project_id", required=True, help="GCP project ID.")
    parser.add_argument("--topic_id", required=True, help="Pub/Sub topic name.")
    parser.add_argument("--event_count", type=int, default=100, help="Number of events to publish.")
    parser.add_argument("--sleep_seconds", type=float, default=0.2, help="Pause between publishes.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible samples.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    publish_events(
        project_id=args.project_id,
        topic_id=args.topic_id,
        event_count=args.event_count,
        sleep_seconds=args.sleep_seconds,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()

