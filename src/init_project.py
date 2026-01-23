import os
import sys
from datetime import datetime, timedelta
import random
from faker import Faker

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.generator import CustomerGenerator, ProductGenerator, OrderGenerator
from src.chaos_monkey import ChaosMonkey
from src.s3_loader import S3Loader
from src.cleanup_s3 import clean_s3_landing
from dotenv import load_dotenv

load_dotenv()

def init_project():
    print("=== Enterprise Data Quality Monitor: INITIALIZATION (Backfill) ===")
    
    # Initialize S3 Landing Zone (Clean Slate)
    print("\nInitializing Project: Clean S3 Landing Zone...")
    try:
        clean_s3_landing()
    except Exception as e:
        print(f"Warning: S3 Cleanup failed or skipped ({e})")

    DATA_DIR = 'data'
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # Generate Reference Dimensions (Customers, Products)
    print("Generating Reference Data...")
    cust_gen = CustomerGenerator(num_customers=2000)
    df_customers = cust_gen.generate()
    
    prod_gen = ProductGenerator(num_products=200)
    df_products = prod_gen.generate()
    
    path_cust = os.path.join(DATA_DIR, 'customers.csv')
    path_prod = os.path.join(DATA_DIR, 'products.csv')
    
    df_customers.to_csv(path_cust, index=False)
    df_products.to_csv(path_prod, index=False)
    
    # Upload Dimensions
    BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    AWS_ACCESS = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')
    LANDING_PREFIX = os.getenv('S3_LANDING_PREFIX', 'landing')
    
    loader = None
    if BUCKET_NAME and AWS_ACCESS:
        loader = S3Loader(BUCKET_NAME, AWS_ACCESS, AWS_SECRET)
        loader.upload_file(path_cust, f"{LANDING_PREFIX}/customers/customers.csv")
        loader.upload_file(path_prod, f"{LANDING_PREFIX}/products/products.csv")

    # Generate 12 Months of Historical Data (Backfill)
    print("Generating Historical Data (12 Months)...")
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365)
    
    print(f"   Window: {start_date} to {end_date}")
    
    current_date = start_date
    total_orders = 0
    
    while current_date <= end_date:
        str_date = current_date.strftime('%Y-%m-%d')
        
        # Generate Orders
        order_gen = OrderGenerator(df_customers, df_products, num_orders=random.randint(50, 150))
        df_orders = order_gen.generate(date=str_date)
        
        # Inject Chaos
        chaos = ChaosMonkey(error_rate=0.10)
        df_orders_dirty = chaos.apply_chaos(df_orders)
        
        # Save & Upload
        filename = f'orders_{str_date}.csv'
        path_orders = os.path.join(DATA_DIR, filename)
        df_orders_dirty.to_csv(path_orders, index=False)
        
        if loader:
            loader.upload_file(path_orders, f"{LANDING_PREFIX}/orders/{filename}")
        
        if current_date.day == 1:
            print(f"   -> Processed month: {current_date.strftime('%Y-%m')}")
            
        total_orders += len(df_orders_dirty)
        current_date += timedelta(days=1)

    print(f"\nSetting Watermark to {end_date}...")
    # Initialize watermark to enable incremental processing from tomorrow
    watermark_file = os.path.join(DATA_DIR, 'watermark.txt')
    with open(watermark_file, 'w') as f:
        f.write(end_date.strftime('%Y-%m-%d'))
        
    print(f"   Watermark set to: {end_date}")

    if loader:
        loader.upload_file(watermark_file, 'watermark.txt')

    print(f"\n=== Initialization Complete ({total_orders} orders generated) ===")

if __name__ == "__main__":
    init_project()
