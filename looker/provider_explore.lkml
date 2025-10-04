explore: provider_segmentation {
  label: "Provider Segmentation"
  description: "Segment providers by cycle, revenue, cost-to-serve, profit, and recency."
  hidden: no

    sets: {
    kpis: [revenue_total, total_cost_to_serve, profit_total, profit_margin, payments_count, orders_count]
  }
}

explore: provider_cycle_analysis {
  label: "Cycle Analysis (Monthly)"
  description: "Monthly time series and KPIs by subscription cycle."

  sets: {
    kpis: [cycle_revenue_total, cycle_cost_total, cycle_profit_total, cycle_profit_margin, payments_count, providers_in_cycle_month]
  }
}
