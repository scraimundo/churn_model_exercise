
CREATE SCHEMA IF NOT EXISTS `striking-coil-474009-u5.ai_churn` OPTIONS(location='EU');

CREATE TABLE IF NOT EXISTS `striking-coil-474009-u5.ai_churn.ai_churn_training_data`
(
  provider_id STRING NOT NULL OPTIONS(description="Provider identifier"),
  snapshot_date DATE NOT NULL OPTIONS(description="As-of date (all features use data on/before this date.)"),
  churn_label INT64 NOT NULL OPTIONS(description="1=no orders AND no payments within 90d after expected cycle end; 0 otherwise; NULL if insufficient history."),
  last_order_date DATE,
  days_since_last_order INT64,
  last_cycle_months INT64,
  expected_last_cycle_end_date DATE,
  total_revenue_last_90d NUMERIC,
  avg_profit_last_90d NUMERIC,
  num_payments_last_90d INT64,
  total_orders_last_180d INT64,
  distinct_order_types_last_180d INT64,
  feature_generation_timestamp TIMESTAMP NOT NULL
)
PARTITION BY snapshot_date
CLUSTER BY provider_id;
