import os
import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.generator import OrderGenerator
from src.chaos_monkey import ChaosMonkey
from src.s3_loader import S3Loader

# Load environment variables
load_dotenv()

def main():
    print("=== Enterprise Data Quality Monitor: Daily Incremental Ingestion ===")
    DATA_DIR = 'data'
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    # 1. Load Dimension Data (Reuse existing to maintain Referential Integrity)
    # The Init flow created these. We just load them to generate valid orders.
    path_cust = os.path.join(DATA_DIR, 'customers.csv')
    path_prod = os.path.join(DATA_DIR, 'products.csv')
    
    if not os.path.exists(path_cust) or not os.path.exists(path_prod):
        print("ERROR: Dimensions not found. Please run 'python src/init_project.py' first.")
        sys.exit(1)
        
    import pandas as pd
    df_customers = pd.read_csv(path_cust)
    df_products = pd.read_csv(path_prod)
    print(f"Loaded {len(df_customers)} customers and {len(df_products)} products.")

    # 2. Determine Time Window (Incremental Strategy)
    watermark_file = os.path.join(DATA_DIR, 'watermark.txt')
    
    if os.path.exists(watermark_file):
        with open(watermark_file, 'r') as f:
            last_run_date_str = f.read().strip()
        last_run_date = datetime.strptime(last_run_date_str, '%Y-%m-%d').date()
    else:
        print("WARNING: No watermark found. Assuming first run or manual reset.")
        # If no watermark, maybe default to Yesterday? Or fail? 
        # User requested flow: "Rellene desde el ultimo dia". 
        # For safety, let's default to Yesterday so we generate Today.
        last_run_date = datetime.now().date() - timedelta(days=1)

    today = datetime.now().date()
    start_date = last_run_date + timedelta(days=1)
    end_date = today

    print(f"Last Run: {last_run_date}")
    print(f"Target Window: {start_date} to {end_date}")

    if start_date > end_date:
        print("✅ System is up to date. No new data to generate.")
        print(f"   (Today is {today}, Last run was {last_run_date})")
        return

    # 3. Generate Data for Window
    current_date = start_date
    
    BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    AWS_ACCESS = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')
    LANDING_PREFIX = os.getenv('S3_LANDING_PREFIX', 'landing')
    
    loader = None
    if BUCKET_NAME and AWS_ACCESS:
        loader = S3Loader(BUCKET_NAME, AWS_ACCESS, AWS_SECRET)

    total_orders_generated = 0

    while current_date <= end_date:
        str_date = current_date.strftime('%Y-%m-%d')
        print(f"  PROCESSING DATE: {str_date}")
        
        # Generator
        order_gen = OrderGenerator(df_customers, df_products, num_orders=random.randint(50, 200))
        df_orders = order_gen.generate(date=str_date)
        
        # Chaos Monkey
        chaos = ChaosMonkey(error_rate=0.10)
        df_orders_dirty = chaos.apply_chaos(df_orders)
        
        # Save & Upload
        filename = f'orders_{str_date}.csv'
        path_orders = os.path.join(DATA_DIR, filename)
        df_orders_dirty.to_csv(path_orders, index=False)
        
        if loader:
            loader.upload_file(path_orders, f"{LANDING_PREFIX}/orders/{filename}")
            print(f"    -> Uploaded {filename}")
        
        total_orders_generated += len(df_orders_dirty)
        current_date += timedelta(days=1)

    # 4. Update Watermark
    with open(watermark_file, 'w') as f:
        f.write(today.strftime('%Y-%m-%d'))
        
    print(f"\nTotal Generated: {total_orders_generated} orders.")
    print(f"Watermark updated to: {today}")
    print("=== Daily Ingestion Phase Complete ===")

if __name__ == "__main__":
    main()
