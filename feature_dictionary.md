# AI Churn — Feature Dictionary

**Table:** `striking-coil-474009-u5.ai_churn.ai_churn_training_data`  
**Grain:** one row per `provider_id` and `snapshot_date`    
**Partitioning:** `PARTITION BY snapshot_date` (cluster by `provider_id`)  
**Data as-of:** all features use data on/before `snapshot_date`  
**Label timeline:** `churn_window_days = 90`

---

## Churn label

- **`churn_label`**  
  1 = no orders in the 90 days after `expected_last_cycle_end_date`;  
  0 = at least one order within that window.

- **Insufficient observation is excluded**: rows where - no last order or `expected_last_cycle_end_date + 90d > snapshot_date` - are not inserted (`WHERE lb.churn_label IS NOT NULL`).

- **Expected cycle end** = `last_order_date + last_cycle_months`.

---

## Fields

| Field | Type | Window | Definition |
|---|---|---:|---|
| `provider_id` | STRING | - | Provider Identifier |
| `snapshot_date` | DATE | - | As-of date; features use data ≤ this date |
| `churn_label` | INT64 | +90d | 1/0 per rule above (only labeled rows inserted) |
| `last_order_date` | DATE | history | Most recent order date ≤ `snapshot_date` |
| `days_since_last_order` | INT64 | as-of | `snapshot_date - last_order_date` (days); NULL if no orders |
| `last_cycle_months` | INT64 | history | Cycle length (3/6/9) of the last order |
| `expected_last_cycle_end_date` | DATE | derived | `last_order_date + last_cycle_months` |
| `total_revenue_last_90d` | NUMERIC | 90d | `SUM(total_amount)` for payments in `(snapshot_date-89d, snapshot_date]` |
| `avg_profit_last_90d` | NUMERIC | 90d | `AVG(profit_amount)` for payments in the same 90d window |
| `num_payments_last_90d` | INT64 | 90d | `COUNT(payments)` in the same 90d window |
| `total_orders_last_180d` | INT64 | 180d | `COUNT(orders)` in `(snapshot_date-179d, snapshot_date]`. |
| `distinct_order_types_last_180d` | INT64 | 180d | `COUNT(DISTINCT order_type)` in that 180d window |
| `feature_generation_timestamp` | TIMESTAMP | - | ETL load timestamp |

---

## Notes

- **Leakage control:** every filter/window uses `snapshot_date` as the time fence. The label looks forward 90d from the expected cycle end only.  
- **Reproducibility:** re-run with a different `snapshot_date` to get a new partition with identical semantics.  
- **Extensibility:** add more windows (e.g. 30/60/180d margins) by following the same `… <= snapshot_date` pattern.  
- **Training vs scoring:**  
  - **Training:** build multiple `snapshot_date` partitions (historical).  
  - **Scoring:** compute same features for yesterday’s `snapshot_date`, drop the label, feed to the model.
