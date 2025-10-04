CREATE SCHEMA IF NOT EXISTS `striking-coil-474009-u5.staging` OPTIONS(location='EU');

-- Table for raw payment data
CREATE TABLE IF NOT EXISTS `striking-coil-474009-u5.staging.stg_payments`
(
    payment_id STRING NOT NULL,
    provider_id STRING NOT NULL,
    payment_date DATE NOT NULL,
    copay_amount NUMERIC,
    insurance_amount NUMERIC,
    total_amount NUMERIC,
    cost_amount NUMERIC,
    profit_amount NUMERIC,
    load_timestamp TIMESTAMP NOT NULL 
)
PARTITION BY DATE_TRUNC(payment_date, MONTH)
CLUSTER BY provider_id;

-- Table for raw order data
CREATE TABLE IF NOT EXISTS `striking-coil-474009-u5.staging.stg_orders`
(
    provider_id STRING NOT NULL,
    order_id STRING NOT NULL,
    order_date DATE NOT NULL,
    order_type STRING,
    subscription_cycle_months INT64, 
    load_timestamp TIMESTAMP NOT NULL 
)
PARTITION BY DATE_TRUNC(order_date, MONTH)
CLUSTER BY provider_id;