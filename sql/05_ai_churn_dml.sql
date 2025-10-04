DECLARE snapshot_date DATE DEFAULT '2025-09-30';
DECLARE churn_window_days INT64 DEFAULT 90;

INSERT INTO `striking-coil-474009-u5.ai_churn.ai_churn_training_data` (
  provider_id,
  snapshot_date,
  churn_label,
  last_order_date,
  days_since_last_order,
  last_cycle_months,
  expected_last_cycle_end_date,
  total_revenue_last_90d,
  avg_profit_last_90d,
  num_payments_last_90d,
  total_orders_last_180d,
  distinct_order_types_last_180d,
  feature_generation_timestamp
)
WITH last_order AS (
  SELECT
    o.provider_id,
    o.order_date AS last_order_date,
    o.subscription_cycle_months AS last_cycle_months,
    DATE_ADD(o.order_date, INTERVAL o.subscription_cycle_months MONTH) AS expected_last_cycle_end_date
  FROM `striking-coil-474009-u5.dwh.fact_orders` o
  WHERE o.order_date <= snapshot_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY o.provider_id ORDER BY o.order_date DESC, o.dw_load_timestamp DESC) = 1
),
payments_90d AS (
  SELECT
    p.provider_id,
    SUM(IF(p.payment_date BETWEEN DATE_SUB(snapshot_date, INTERVAL 89 DAY) AND snapshot_date, p.total_amount, 0))  AS total_revenue_last_90d,
    AVG(IF(p.payment_date BETWEEN DATE_SUB(snapshot_date, INTERVAL 89 DAY) AND snapshot_date, p.profit_amount, NULL)) AS avg_profit_last_90d,
    COUNTIF(p.payment_date BETWEEN DATE_SUB(snapshot_date, INTERVAL 89 DAY) AND snapshot_date) AS num_payments_last_90d
  FROM `striking-coil-474009-u5.dwh.fact_payments` p
  WHERE p.payment_date <= snapshot_date
  GROUP BY 1
),
orders_180d AS (
  SELECT
    o.provider_id,
    COUNTIF(o.order_date BETWEEN DATE_SUB(snapshot_date, INTERVAL 179 DAY) AND snapshot_date) AS total_orders_last_180d,
    COUNT(DISTINCT IF(o.order_date BETWEEN DATE_SUB(snapshot_date, INTERVAL 179 DAY) AND snapshot_date, o.order_type, NULL)) AS distinct_order_types_last_180d
  FROM `striking-coil-474009-u5.dwh.fact_orders` o
  WHERE o.order_date <= snapshot_date
  GROUP BY 1
),
label AS (
  SELECT
    dp.provider_id,
    CASE
      WHEN lo.last_order_date IS NULL THEN NULL
      WHEN DATE_ADD(lo.expected_last_cycle_end_date, INTERVAL churn_window_days DAY) > snapshot_date THEN NULL
      WHEN NOT EXISTS (
        SELECT 1
        FROM `striking-coil-474009-u5.dwh.fact_orders` o
        WHERE o.provider_id = dp.provider_id
          AND o.order_date > lo.expected_last_cycle_end_date
          AND o.order_date <= DATE_ADD(lo.expected_last_cycle_end_date, INTERVAL churn_window_days DAY)
      ) THEN 1
      ELSE 0
    END AS churn_label
  FROM `striking-coil-474009-u5.dwh.dim_providers` dp
  LEFT JOIN last_order lo ON lo.provider_id = dp.provider_id
)
SELECT
  dp.provider_id,
  snapshot_date AS snapshot_date,
  lb.churn_label,  
  lo.last_order_date,
  CASE WHEN lo.last_order_date IS NULL THEN NULL ELSE DATE_DIFF(snapshot_date, lo.last_order_date, DAY) END AS days_since_last_order,
  lo.last_cycle_months,
  lo.expected_last_cycle_end_date,
  COALESCE(p90.total_revenue_last_90d, 0) AS total_revenue_last_90d,
  COALESCE(p90.avg_profit_last_90d, 0)    AS avg_profit_last_90d,
  COALESCE(p90.num_payments_last_90d, 0)  AS num_payments_last_90d,
  COALESCE(o180.total_orders_last_180d, 0)         AS total_orders_last_180d,
  COALESCE(o180.distinct_order_types_last_180d, 0) AS distinct_order_types_last_180d,
  CURRENT_TIMESTAMP() AS feature_generation_timestamp
  
FROM `striking-coil-474009-u5.dwh.dim_providers` dp
LEFT JOIN last_order lo ON lo.provider_id = dp.provider_id
LEFT JOIN payments_90d p90 ON p90.provider_id = dp.provider_id
LEFT JOIN orders_180d o180 ON o180.provider_id = dp.provider_id
LEFT JOIN label lb ON lb.provider_id = dp.provider_id
WHERE lb.churn_label IS NOT NULL
;
