CREATE OR REPLACE TABLE `your-gcp-project-id.gold.store_hourly_kpis`
PARTITION BY DATE(window_hour)
CLUSTER BY region, store_id AS
SELECT
  TIMESTAMP_TRUNC(order_created_ts, HOUR) AS window_hour,
  store_id,
  region,
  COUNT(*) AS total_orders,
  ROUND(SUM(order_amount), 2) AS gross_revenue,
  ROUND(AVG(order_amount), 2) AS avg_order_value,
  COUNTIF(payment_status = 'CAPTURED') AS payment_captured_orders,
  COUNTIF(dispatch_status = 'DISPATCHED') AS dispatched_orders,
  COUNTIF(delivered_within_sla) AS sla_orders,
  ROUND(SAFE_DIVIDE(COUNTIF(delivered_within_sla), COUNT(*)) * 100, 2) AS sla_pct
FROM `your-gcp-project-id.silver.orders_curated`
GROUP BY 1, 2, 3;

CREATE OR REPLACE TABLE `your-gcp-project-id.gold.region_fulfilment_sla`
PARTITION BY order_date
CLUSTER BY region AS
SELECT
  order_date,
  region,
  COUNT(*) AS total_orders,
  COUNTIF(dispatch_lag_minutes <= 120) AS dispatched_within_2_hours,
  COUNTIF(delivered_within_sla) AS delivered_within_sla,
  ROUND(SAFE_DIVIDE(COUNTIF(dispatch_lag_minutes <= 120), COUNT(*)) * 100, 2) AS dispatch_within_2_hours_pct,
  ROUND(SAFE_DIVIDE(COUNTIF(delivered_within_sla), COUNT(*)) * 100, 2) AS delivered_within_sla_pct,
  ROUND(AVG(dispatch_lag_minutes), 2) AS avg_dispatch_lag_minutes,
  ROUND(AVG(delivery_cycle_hours), 2) AS avg_delivery_cycle_hours
FROM `your-gcp-project-id.silver.orders_curated`
GROUP BY 1, 2;

