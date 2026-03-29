CREATE OR REPLACE TABLE `your-gcp-project-id.silver.orders_curated`
PARTITION BY order_date
CLUSTER BY region, store_id AS
WITH latest_order_created AS (
  SELECT
    order_id,
    customer_id,
    store_id,
    region,
    currency,
    order_amount,
    DATE(event_ts) AS order_date,
    event_ts AS order_created_ts,
    items
  FROM `your-gcp-project-id.bronze.retail_events`
  WHERE event_type = 'ORDER_CREATED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts DESC) = 1
),
latest_payment AS (
  SELECT
    order_id,
    payment_status,
    event_ts AS payment_ts
  FROM `your-gcp-project-id.bronze.retail_events`
  WHERE event_type = 'PAYMENT_CAPTURED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts DESC) = 1
),
latest_dispatch AS (
  SELECT
    order_id,
    shipment_status,
    event_ts AS shipment_dispatched_ts
  FROM `your-gcp-project-id.bronze.retail_events`
  WHERE event_type = 'SHIPMENT_DISPATCHED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts DESC) = 1
),
latest_delivery AS (
  SELECT
    order_id,
    shipment_status,
    event_ts AS delivery_completed_ts
  FROM `your-gcp-project-id.bronze.retail_events`
  WHERE event_type = 'DELIVERY_COMPLETED'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_ts DESC) = 1
)
SELECT
  created.order_id,
  created.customer_id,
  created.store_id,
  created.region,
  created.currency,
  created.order_amount,
  created.order_date,
  created.order_created_ts,
  payment.payment_status,
  payment.payment_ts,
  dispatch.shipment_status AS dispatch_status,
  dispatch.shipment_dispatched_ts,
  delivery.shipment_status AS delivery_status,
  delivery.delivery_completed_ts,
  TIMESTAMP_DIFF(dispatch.shipment_dispatched_ts, created.order_created_ts, MINUTE) AS dispatch_lag_minutes,
  TIMESTAMP_DIFF(delivery.delivery_completed_ts, created.order_created_ts, HOUR) AS delivery_cycle_hours,
  IF(
    delivery.delivery_completed_ts IS NOT NULL
    AND TIMESTAMP_DIFF(delivery.delivery_completed_ts, created.order_created_ts, HOUR) <= 48,
    TRUE,
    FALSE
  ) AS delivered_within_sla,
  created.items
FROM latest_order_created AS created
LEFT JOIN latest_payment AS payment
  ON created.order_id = payment.order_id
LEFT JOIN latest_dispatch AS dispatch
  ON created.order_id = dispatch.order_id
LEFT JOIN latest_delivery AS delivery
  ON created.order_id = delivery.order_id;
