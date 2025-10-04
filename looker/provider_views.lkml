
view: provider_segmentation {
  label: "Provider Segmentation"
  sql_table_name: `striking-coil-474009-u5.reporting.provider_segmentation` ;;

  dimension: provider_id { 
    primary_key: yes 
    type: string 
    sql: ${TABLE}.provider_id ;; 
    }

  dimension: current_cycle_months { 
    type: number 
    sql: ${TABLE}.current_cycle_months ;; 
    }
  dimension: days_since_last_order { 
    type: number 
    sql: ${TABLE}.days_since_last_order ;; 
    }
  dimension: days_since_last_payment { 
    type: number 
    sql: ${TABLE}.days_since_last_payment ;; 
    }


  measure: revenue_total { 
    type: sum 
    sql: ${TABLE}.revenue_total ;; 
    value_format_name: "usd_0" }

  measure: total_cost_to_serve { 
    type: sum 
    sql: ${TABLE}.total_cost_to_serve ;; 
    value_format_name: "usd_0" 
    }

  measure: profit_total { 
    type: sum 
    sql: ${TABLE}.profit_total ;; 
    value_format_name: "usd_0" 
    }

  measure: payments_count { 
    type: sum 
    sql: ${TABLE}.payments_count ;; 
    }

  measure: orders_count { 
    type: sum 
    sql: ${TABLE}.orders_count ;; 
    }

  measure: profit_margin {
    type: number
    sql: SAFE_DIVIDE(${profit_total}, NULLIF(${revenue_total}, 0)) ;;
    value_format_name: "percent_1"
  }


  drill_fields: [provider_id, current_cycle_months, revenue_total, total_cost_to_serve, profit_total, payments_count, orders_count, days_since_last_order, days_since_last_payment]
}


view: provider_cycle_analysis {
  label: "Provider Cycle Analysis"
  sql_table_name: `striking-coil-474009-u5.reporting.provider_cycle_analysis` ;;

  dimension_group: month {
    type: time
    timeframes: [raw, month, year]
    sql: ${TABLE}.month ;;
  }

  dimension: cycle_months { 
    type: number 
    sql: ${TABLE}.cycle_months ;; 
    }

  measure: cycle_revenue_total { 
    type: sum 
    sql: ${TABLE}.cycle_revenue_total ;; 
    value_format_name: "usd_0" 
    }

  measure: cycle_cost_total    { 
    type: sum 
    sql: ${TABLE}.cycle_cost_total    ;; 
    value_format_name: "usd_0" 
    }

  measure: cycle_profit_total  { 
    type: sum 
    sql: ${TABLE}.cycle_profit_total  ;; 
    value_format_name: "usd_0" 
    }

  measure: payments_count      { 
    type: sum 
    sql: ${TABLE}.payments_count      ;; 
    }

  measure: providers_in_cycle_month { 
    type: sum 
    sql: ${TABLE}.providers_in_cycle_month ;; 
    }

  measure: cycle_profit_margin {
    type: number
    sql: SAFE_DIVIDE(${cycle_profit_total}, NULLIF(${cycle_revenue_total}, 0)) ;;
    value_format_name: "percent_1"
  }
}
