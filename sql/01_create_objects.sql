DECLARE project_id STRING DEFAULT 'your-gcp-project-id';

EXECUTE IMMEDIATE FORMAT("""
CREATE SCHEMA IF NOT EXISTS `%s.bronze`
OPTIONS(location = 'US')
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE SCHEMA IF NOT EXISTS `%s.silver`
OPTIONS(location = 'US')
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE SCHEMA IF NOT EXISTS `%s.gold`
OPTIONS(location = 'US')
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE SCHEMA IF NOT EXISTS `%s.ops`
OPTIONS(location = 'US')
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.bronze.retail_events`
(
  event_id STRING NOT NULL,
  order_id STRING NOT NULL,
  customer_id STRING,
  event_type STRING NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  event_date DATE NOT NULL,
  store_id STRING NOT NULL,
  region STRING NOT NULL,
  currency STRING NOT NULL,
  order_amount FLOAT64 NOT NULL,
  payment_status STRING,
  shipment_status STRING,
  ingest_source STRING NOT NULL,
  processing_ts TIMESTAMP NOT NULL,
  is_late_arrival BOOL NOT NULL,
  items ARRAY<STRUCT<
    sku STRING,
    category STRING,
    qty INT64,
    unit_price FLOAT64,
    line_amount FLOAT64
  >>
)
PARTITION BY event_date
CLUSTER BY event_type, store_id, region
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.ops.real_time_order_metrics`
(
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  store_id STRING NOT NULL,
  region STRING NOT NULL,
  total_orders INT64 NOT NULL,
  gross_revenue FLOAT64 NOT NULL,
  avg_order_value FLOAT64 NOT NULL,
  high_value_orders INT64 NOT NULL,
  pipeline_run_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY store_id, region
""", project_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.ops.data_quality_audit`
(
  run_date DATE NOT NULL,
  check_name STRING NOT NULL,
  status STRING NOT NULL,
  failed_rows INT64 NOT NULL,
  details STRING,
  created_ts TIMESTAMP NOT NULL
)
PARTITION BY run_date
CLUSTER BY check_name, status
""", project_id);

