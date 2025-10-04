CREATE SCHEMA IF NOT EXISTS `striking-coil-474009-u5.dwh` OPTIONS (location = 'EU');

-- Dim
CREATE TABLE IF NOT EXISTS `striking-coil-474009-u5.dwh.dim_providers` (
  provider_id STRING NOT NULL,
  dw_load_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (provider_id) NOT ENFORCED
);

-- Facts
CREATE TABLE IF NOT EXISTS `striking-coil-474009-u5.dwh.fact_payments` (
  payment_id STRING NOT NULL,
  provider_id STRING NOT NULL,
  payment_date DATE NOT NULL,
  copay_amount NUMERIC,
  insurance_amount NUMERIC,
  total_amount NUMERIC,
  cost_amount NUMERIC,
  profit_amount NUMERIC,
  dw_load_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (payment_id) NOT ENFORCED,
  FOREIGN KEY (provider_id) REFERENCES `striking-coil-474009-u5.dwh.dim_providers`(provider_id) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(payment_date, MONTH)
CLUSTER BY provider_id;

CREATE TABLE IF NOT EXISTS `striking-coil-474009-u5.dwh.fact_orders` (
  order_id STRING NOT NULL,
  provider_id STRING NOT NULL,
  order_date DATE NOT NULL,
  order_type STRING,
  subscription_cycle_months INT64 NOT NULL,
  dw_load_timestamp TIMESTAMP NOT NULL,
  PRIMARY KEY (order_id) NOT ENFORCED,
  FOREIGN KEY (provider_id) REFERENCES `striking-coil-474009-u5.dwh.dim_providers`(provider_id) NOT ENFORCED
)
PARTITION BY DATE_TRUNC(order_date, MONTH)
CLUSTER BY provider_id;

-- DML
MERGE `striking-coil-474009-u5.dwh.dim_providers` T
USING (
  SELECT DISTINCT provider_id FROM `striking-coil-474009-u5.staging.stg_payments`
  UNION DISTINCT
  SELECT DISTINCT provider_id FROM `striking-coil-474009-u5.staging.stg_orders`
) S
ON T.provider_id = S.provider_id
WHEN NOT MATCHED THEN
  INSERT (provider_id, dw_load_timestamp) VALUES (S.provider_id, CURRENT_TIMESTAMP());

MERGE `striking-coil-474009-u5.dwh.fact_payments` T
USING (
  SELECT
    s.payment_id,
    s.provider_id,
    s.payment_date,
    s.copay_amount,
    s.insurance_amount,
    s.total_amount,
    s.cost_amount,
    s.profit_amount,
    s.load_timestamp AS dw_load_timestamp
  FROM `striking-coil-474009-u5.staging.stg_payments` s
  WHERE s.payment_date IS NOT NULL
) S
ON T.payment_id = S.payment_id
WHEN NOT MATCHED THEN
  INSERT (payment_id, provider_id, payment_date, copay_amount, insurance_amount, total_amount, cost_amount, profit_amount, dw_load_timestamp)
  VALUES (S.payment_id, S.provider_id, S.payment_date, S.copay_amount, S.insurance_amount, S.total_amount, S.cost_amount, S.profit_amount, S.dw_load_timestamp);

MERGE `striking-coil-474009-u5.dwh.fact_orders` T
USING (
  SELECT
    s.order_id,
    s.provider_id,
    s.order_date,
    s.order_type,
    s.subscription_cycle_months,
    s.load_timestamp AS dw_load_timestamp
  FROM `striking-coil-474009-u5.staging.stg_orders` s
  WHERE s.order_date IS NOT NULL
) S
ON T.order_id = S.order_id
WHEN NOT MATCHED THEN
  INSERT (order_id, provider_id, order_date, order_type, subscription_cycle_months, dw_load_timestamp)
  VALUES (S.order_id, S.provider_id, S.order_date, S.order_type, S.subscription_cycle_months, S.dw_load_timestamp);
