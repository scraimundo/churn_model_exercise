import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def generate_sample_data(num_providers=50, num_payments_per_provider=3, num_orders_per_provider=2):
    """Generates sample payments and orders data."""

    providers = [f"P_{i:04d}" for i in range(1, num_providers + 1)]
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 10, 2)

    # Payments random data 
    payments_data = []
    for provider_id in providers:
        pay_idx = 0  
        for _ in range(np.random.randint(1, num_payments_per_provider + 1)):
            payment_date = start_date + timedelta(days=np.random.randint(0, (end_date - start_date).days))
            copay = round(np.random.uniform(10, 100), 2)
            insurance = round(np.random.uniform(50, 500), 2)
            total = copay + insurance
            cost = round(total * np.random.uniform(0.5, 0.8), 2)
            profit = round(total - cost, 2)
            payment_id = f"PM_{provider_id.split('_')[1]}_{pay_idx:04d}" 
            payments_data.append({
            	"payment_id": payment_id, 
            	"provider_id": provider_id,
            	"payment_date": payment_date.strftime("%Y-%m-%d"),
            	"copay_amount": copay,
            	"insurance_amount": insurance,
            	"total_amount": total,
            	"cost_amount": cost,
            	"profit_amount": profit
            })
            pay_idx += 1
    df_payments = pd.DataFrame(payments_data)

    # Orders random data 
    order_types = ["cycle_3m", "cycle_6m", "cycle_9m"]
    orders_data = []
    for provider_id in providers:
        for i in range(np.random.randint(1, num_orders_per_provider + 1)): 
            order_date = start_date + timedelta(days=np.random.randint(0, (end_date - start_date).days))
            order_type = np.random.choice(order_types, p=[0.5, 0.3, 0.2]) 
            orders_data.append({
                "provider_id": provider_id,
                "order_id": f"O_{provider_id.split('_')[1]}_{i:03d}",
                "order_date": order_date.strftime("%Y-%m-%d"),
                "order_type": order_type
            })
    df_orders = pd.DataFrame(orders_data)

    # Save data to raw directory 
    output_dir = "raw"
    os.makedirs(output_dir, exist_ok=True)

    payments_path = os.path.join(output_dir, "payments.csv")
    orders_path = os.path.join(output_dir, "orders.csv")

    df_payments.to_csv(payments_path, index=False)
    df_orders.to_csv(orders_path, index=False)

    print(f"Generated {len(df_payments)} payments records and {len(df_orders)} orders records.")
    print(f"Saved to {payments_path} and {orders_path}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(current_dir)
    generate_sample_data()