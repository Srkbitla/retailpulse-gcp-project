import json
import unittest
from datetime import datetime, timedelta, timezone

from src.streaming.transforms import build_metric_seed, normalize_event, parse_message, validate_event


class TransformTests(unittest.TestCase):
    def setUp(self) -> None:
        base_time = datetime.now(timezone.utc) - timedelta(minutes=2)
        self.order_event = {
            "event_id": "evt-1001",
            "order_id": "ord-1001",
            "customer_id": "cust-1001",
            "event_type": "order_created",
            "event_ts": base_time.isoformat().replace("+00:00", "Z"),
            "store_id": "store_101",
            "region": "north",
            "currency": "usd",
            "order_amount": 145.5,
            "payment_status": "pending",
            "shipment_status": "pending",
            "ingest_source": "unit-test",
            "items": [
                {
                    "sku": "ELE-2001",
                    "category": "electronics",
                    "qty": 1,
                    "unit_price": 145.5,
                }
            ],
        }

    def test_parse_message_from_bytes(self) -> None:
        payload = json.dumps(self.order_event).encode("utf-8")
        parsed = parse_message(payload)
        self.assertEqual(parsed["order_id"], "ord-1001")

    def test_normalize_event_sets_expected_fields(self) -> None:
        processed_at = datetime.now(timezone.utc)
        normalized = normalize_event(self.order_event, processed_at=processed_at)
        self.assertEqual(normalized["event_type"], "ORDER_CREATED")
        self.assertEqual(normalized["region"], "NORTH")
        self.assertEqual(normalized["currency"], "USD")
        self.assertIn("event_date", normalized)
        self.assertFalse(normalized["is_late_arrival"])

    def test_validate_event_rejects_invalid_event_type(self) -> None:
        bad_event = dict(self.order_event)
        bad_event["event_type"] = "refund_completed"
        errors = validate_event(bad_event)
        self.assertTrue(any("Unsupported event_type" in error for error in errors))

    def test_build_metric_seed_for_order_created(self) -> None:
        normalized = normalize_event(self.order_event)
        metric_seed = build_metric_seed(normalized)
        self.assertEqual(metric_seed["order_count"], 1)
        self.assertEqual(metric_seed["gross_revenue"], 145.5)
        self.assertEqual(metric_seed["high_value_orders"], 0)

    def test_validate_event_rejects_non_positive_order_amount(self) -> None:
        bad_event = dict(self.order_event)
        bad_event["order_amount"] = 0
        errors = validate_event(bad_event)
        self.assertTrue(any("order_amount must be greater than zero" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
