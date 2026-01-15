import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='RAW'
    )

import argparse

def run_load(mode='incremental'):
    print(f"Connecting to Snowflake for Loading (Mode: {mode.upper()})...")
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    
    try:
        # Load logic
        if mode == 'full':
            print("  [FULL] Truncating RAW Tables (Clean Slate)...")
            cursor.execute("TRUNCATE TABLE RAW.RAW_CUSTOMERS")
            cursor.execute("TRUNCATE TABLE RAW.RAW_PRODUCTS")
            cursor.execute("TRUNCATE TABLE RAW.RAW_ORDERS")
            force_option = "FORCE = TRUE"
        else:
            print("  [INCREMENTAL] Skipping Truncate (Appending new files)...")
            # Incremental load: Append only. Dimensions treated as Type 1 (overwrite not supported in this simple loader).
            force_option = "FORCE = FALSE"

        print(f"Loading Customers ({force_option})...")
        cursor.execute(f"""
            COPY INTO raw_customers
            FROM @s3_landing_stage/customers/
            FILE_FORMAT = (FORMAT_NAME = csv_format)
            ON_ERROR = 'CONTINUE'
            {force_option}
        """)
        
        print(f"Loading Products ({force_option})...")
        cursor.execute(f"""
            COPY INTO raw_products
            FROM @s3_landing_stage/products/
            FILE_FORMAT = (FORMAT_NAME = csv_format)
            ON_ERROR = 'CONTINUE'
            {force_option}
        """)
        
        print(f"Loading Orders ({force_option})...")
        cursor.execute(f"""
            COPY INTO raw_orders
            FROM @s3_landing_stage/orders/
            FILE_FORMAT = (FORMAT_NAME = csv_format)
            ON_ERROR = 'CONTINUE'
            {force_option}
        """)
        
        print("Data Load Completed.")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['full', 'incremental'], default='incremental', help='Load mode: full (truncate) or incremental (append)')
    args = parser.parse_args()
    
    run_load(mode=args.mode)
