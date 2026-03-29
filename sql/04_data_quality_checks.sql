DECLARE run_date DATE DEFAULT CURRENT_DATE();

INSERT INTO `your-gcp-project-id.ops.data_quality_audit`
(run_date, check_name, status, failed_rows, details, created_ts)
SELECT
  run_date,
  'null_order_id_in_silver',
  IF(COUNT(*) = 0, 'PASS', 'FAIL'),
  COUNT(*),
  'Orders curated table should not contain null order_id values.',
  CURRENT_TIMESTAMP()
FROM `your-gcp-project-id.silver.orders_curated`
WHERE order_id IS NULL;

INSERT INTO `your-gcp-project-id.ops.data_quality_audit`
(run_date, check_name, status, failed_rows, details, created_ts)
SELECT
  run_date,
  'duplicate_order_id_in_silver',
  IF(COUNT(*) = 0, 'PASS', 'FAIL'),
  COUNT(*),
  'Each order_id should be unique in silver.orders_curated.',
  CURRENT_TIMESTAMP()
FROM (
  SELECT order_id
  FROM `your-gcp-project-id.silver.orders_curated`
  GROUP BY order_id
  HAVING COUNT(*) > 1
);

INSERT INTO `your-gcp-project-id.ops.data_quality_audit`
(run_date, check_name, status, failed_rows, details, created_ts)
SELECT
  run_date,
  'negative_or_zero_revenue',
  IF(COUNT(*) = 0, 'PASS', 'FAIL'),
  COUNT(*),
  'Order amount should be greater than zero for created orders.',
  CURRENT_TIMESTAMP()
FROM `your-gcp-project-id.silver.orders_curated`
WHERE order_amount <= 0;

INSERT INTO `your-gcp-project-id.ops.data_quality_audit`
(run_date, check_name, status, failed_rows, details, created_ts)
SELECT
  run_date,
  'slow_dispatch_over_6_hours',
  IF(COUNT(*) = 0, 'PASS', 'WARN'),
  COUNT(*),
  'Dispatch lag greater than 6 hours indicates an operational bottleneck.',
  CURRENT_TIMESTAMP()
FROM `your-gcp-project-id.silver.orders_curated`
WHERE dispatch_lag_minutes > 360;
