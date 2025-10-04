CREATE SCHEMA IF NOT EXISTS `striking-coil-474009-u5.reporting` OPTIONS (location = 'EU');

-- ------------------------------------------------------------------
-- Provider Segmentation 
-- ------------------------------------------------------------------
CREATE OR REPLACE TABLE `striking-coil-474009-u5.reporting.provider_segmentation` AS
WITH pay AS (
  SELECT
    provider_id,
    COUNT(DISTINCT payment_id)                AS payments_count,
    SUM(total_amount)                         AS revenue_total,
    SUM(cost_amount)                          AS cost_total,
    SUM(profit_amount)                        AS profit_total,
    MAX(payment_date)                         AS last_payment_date,
    MIN(payment_date)                         AS first_payment_date
  FROM `striking-coil-474009-u5.dwh.fact_payments`
  GROUP BY provider_id
),
ord_base AS (
  SELECT
    provider_id,
    COUNT(DISTINCT order_id)                  AS orders_count,
    ARRAY_AGG(DISTINCT subscription_cycle_months IGNORE NULLS) AS distinct_cycle_types,
    MAX(order_date)                           AS last_order_date,
    MIN(order_date)                           AS first_order_date
  FROM `striking-coil-474009-u5.dwh.fact_orders`
  GROUP BY provider_id
),
ord_latest AS (
  SELECT provider_id, subscription_cycle_months AS current_cycle_months
  FROM (
    SELECT
      provider_id,
      subscription_cycle_months,
      ROW_NUMBER() OVER (PARTITION BY provider_id ORDER BY order_date DESC, dw_load_timestamp DESC) AS rn
    FROM `striking-coil-474009-u5.dwh.fact_orders`
  )
  WHERE rn = 1
)
SELECT
  dp.provider_id,
  p.revenue_total,
  p.cost_total        AS total_cost_to_serve,
  p.profit_total,
  SAFE_DIVIDE(p.profit_total, NULLIF(p.revenue_total, 0)) AS profit_margin,
  p.payments_count,
  SAFE_DIVIDE(cost_total, NULLIF(revenue_total, 0)) AS cost_ratio,
  o.orders_count,
  o.distinct_cycle_types,
  o.first_order_date,
  o.last_order_date,
  DATE_DIFF(CURRENT_DATE(), o.last_order_date, DAY) AS days_since_last_order,
  p.first_payment_date,
  p.last_payment_date,
  DATE_DIFF(CURRENT_DATE(), p.last_payment_date, DAY) AS days_since_last_payment,
  ol.current_cycle_months
FROM `striking-coil-474009-u5.dwh.dim_providers` dp
LEFT JOIN pay        p  USING (provider_id)
LEFT JOIN ord_base   o  USING (provider_id)
LEFT JOIN ord_latest ol USING (provider_id);

-- ------------------------------------------------------------------
-- Provider Cycle Analysis (by month & cycle)
-- ------------------------------------------------------------------
CREATE OR REPLACE TABLE `striking-coil-474009-u5.reporting.provider_cycle_analysis` AS
WITH payments_m AS (
  SELECT
    provider_id,
    DATE_TRUNC(payment_date, MONTH) AS month,
    SUM(total_amount)               AS revenue_total,
    SUM(cost_amount)                AS cost_total,
    SUM(profit_amount)              AS profit_total,
    COUNT(DISTINCT payment_id)      AS payments_count
  FROM `striking-coil-474009-u5.dwh.fact_payments`
  GROUP BY provider_id, month
),
orders_m AS (
  SELECT
    provider_id,
    DATE_TRUNC(order_date, MONTH) AS month,
    ANY_VALUE(subscription_cycle_months) AS cycle_months
  FROM `striking-coil-474009-u5.dwh.fact_orders`
  GROUP BY provider_id, month
)
SELECT
  o.cycle_months,
  pm.month,
  COUNT(DISTINCT pm.provider_id)                         AS providers_in_cycle_month,
  SUM(pm.revenue_total)                                  AS cycle_revenue_total,
  SUM(pm.cost_total)                                     AS cycle_cost_total,
  SUM(pm.profit_total)                                   AS cycle_profit_total,
  SAFE_DIVIDE(SUM(pm.profit_total), NULLIF(SUM(pm.revenue_total), 0)) AS cycle_profit_margin,
  SUM(pm.payments_count)                                 AS payments_count
FROM orders_m o
JOIN payments_m pm
  ON pm.provider_id = o.provider_id
 AND pm.month       = o.month
WHERE o.cycle_months IS NOT NULL AND o.cycle_months > 0
GROUP BY o.cycle_months, pm.month;
