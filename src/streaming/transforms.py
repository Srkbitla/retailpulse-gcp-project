from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


VALID_EVENT_TYPES = {
    "ORDER_CREATED",
    "PAYMENT_CAPTURED",
    "SHIPMENT_DISPATCHED",
    "DELIVERY_COMPLETED",
}

REQUIRED_FIELDS = {
    "event_id",
    "order_id",
    "customer_id",
    "event_type",
    "event_ts",
    "store_id",
    "region",
    "currency",
    "order_amount",
    "ingest_source",
}

HIGH_VALUE_ORDER_THRESHOLD = 500.0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso8601(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def parse_message(message: bytes | str) -> dict[str, Any]:
    raw_payload = message.decode("utf-8") if isinstance(message, bytes) else message
    payload = json.loads(raw_payload)

    if not isinstance(payload, dict):
        raise ValueError("The Pub/Sub message must be a JSON object.")

    return payload


def _normalize_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_items = []

    for item in items:
        qty = int(item["qty"])
        unit_price = round(float(item["unit_price"]), 2)
        normalized_items.append(
            {
                "sku": str(item["sku"]),
                "category": str(item["category"]).lower(),
                "qty": qty,
                "unit_price": unit_price,
                "line_amount": round(qty * unit_price, 2),
            }
        )

    return normalized_items


def normalize_event(record: dict[str, Any], processed_at: datetime | None = None) -> dict[str, Any]:
    processed_at = processed_at or utc_now()
    event_ts = parse_timestamp(str(record["event_ts"]))
    items = _normalize_items(record.get("items", []))
    order_amount = round(float(record["order_amount"]), 2)

    return {
        "event_id": str(record["event_id"]),
        "order_id": str(record["order_id"]),
        "customer_id": str(record["customer_id"]),
        "event_type": str(record["event_type"]).upper(),
        "event_ts": to_iso8601(event_ts),
        "event_date": event_ts.date().isoformat(),
        "store_id": str(record["store_id"]),
        "region": str(record["region"]).upper(),
        "currency": str(record["currency"]).upper(),
        "order_amount": order_amount,
        "payment_status": str(record.get("payment_status", "")).upper() or None,
        "shipment_status": str(record.get("shipment_status", "")).upper() or None,
        "ingest_source": str(record["ingest_source"]),
        "processing_ts": to_iso8601(processed_at),
        "is_late_arrival": (processed_at - event_ts).total_seconds() > 300,
        "items": items,
    }


def validate_event(record: dict[str, Any]) -> list[str]:
    errors = []
    missing = sorted(REQUIRED_FIELDS - set(record))

    if missing:
        errors.append(f"Missing required fields: {', '.join(missing)}")
        return errors

    event_type = str(record.get("event_type", "")).upper()
    if event_type not in VALID_EVENT_TYPES:
        errors.append(f"Unsupported event_type: {event_type}")

    try:
        event_ts = parse_timestamp(str(record["event_ts"]))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"Invalid event_ts: {exc}")
        event_ts = None

    try:
        amount = float(record["order_amount"])
        if amount <= 0:
            errors.append("order_amount must be greater than zero")
    except (TypeError, ValueError):
        errors.append("order_amount must be numeric")

    if len(str(record["currency"])) != 3:
        errors.append("currency must be a 3-letter ISO code")

    items = record.get("items", [])
    if not isinstance(items, list):
        errors.append("items must be a list")
    elif event_type == "ORDER_CREATED" and not items:
        errors.append("ORDER_CREATED events must include at least one item")

    for index, item in enumerate(items):
        required_item_fields = {"sku", "category", "qty", "unit_price"}
        item_missing = required_item_fields - set(item)
        if item_missing:
            errors.append(f"items[{index}] is missing fields: {', '.join(sorted(item_missing))}")
            continue

        try:
            qty = int(item["qty"])
            unit_price = float(item["unit_price"])
            if qty <= 0 or unit_price <= 0:
                errors.append(f"items[{index}] must have positive qty and unit_price")
        except (TypeError, ValueError):
            errors.append(f"items[{index}] qty and unit_price must be numeric")

    if event_ts and event_ts > utc_now().replace(microsecond=0):
        errors.append("event_ts cannot be in the future")

    return errors


def format_invalid_record(raw_message: bytes | str, errors: list[str]) -> dict[str, Any]:
    raw_payload = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else str(raw_message)
    return {
        "received_ts": to_iso8601(utc_now()),
        "error_count": len(errors),
        "errors": errors,
        "raw_message": raw_payload,
    }


def build_metric_seed(record: dict[str, Any]) -> dict[str, Any]:
    if record["event_type"] != "ORDER_CREATED":
        raise ValueError("Only ORDER_CREATED events can be used for order metrics.")

    order_amount = round(float(record["order_amount"]), 2)
    return {
        "store_id": record["store_id"],
        "region": record["region"],
        "order_count": 1,
        "gross_revenue": order_amount,
        "high_value_orders": 1 if order_amount >= HIGH_VALUE_ORDER_THRESHOLD else 0,
    }
