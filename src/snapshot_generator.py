import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
import sys

# Add src path to allow imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from s3_loader import S3Loader

load_dotenv()

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='RAW_MARTS'
    )

def generate_dashboard_feed():
    print("Connecting to Snowflake...")
    conn = get_snowflake_conn()
    
    try:
        # Fetch aggregated business metrics (Revenue, Quality Scores)
        query_orders = """
        SELECT 
            order_date, 
            status, 
            count(*) as order_count, 
            sum(total_amount) as total_revenue,
            
            -- Clean Revenue (Excluding all known bad data)
            sum(case 
                when is_orphan_order 
                  or has_negative_amount 
                  or has_math_error 
                  or is_future_order 
                  or has_bad_status 
                  or is_duplicate 
                then 0 
                else total_amount 
            end) as clean_revenue,

            count(case when is_orphan_order then 1 end) as orphan_orders,
            count(case when has_negative_amount then 1 end) as negative_amount_orders,
            count(case when is_duplicate then 1 end) as duplicate_orders,
            count(case when is_future_order then 1 end) as future_orders,
            count(case when has_math_error then 1 end) as math_errors,
            count(case when has_bad_status then 1 end) as bad_status_orders
        FROM RAW_MARTS.FCT_ORDERS
        GROUP BY 1, 2
        """
        print("Fetching Order Metrics...")
        df_orders = pd.read_sql(query_orders, conn)
        
        # Fetch Data Quality Logs from Elementary results
        # Requires Elementary dbt package to be active.
        
        query_dq = """
        SELECT 
            min(generated_at) as test_run_time,
            count(*) as total_failures
        FROM EDQM_DB.RAW.ELEMENTARY_TEST_RESULTS
        WHERE status = 'fail' OR status = 'warn'
        """
        # We might need to adjust schema/table based on Elementary version
        
        print("Fetching DQ Metrics (Placeholder if Elementary not ready)...")
        # Save snapshot for dashboard ingestion.
        
        output_path = 'dashboard_feed.csv'
        df_orders.to_csv(output_path, index=False)
        print(f"Snapshot saved to {output_path}")

        # Upload to S3
        BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
        AWS_ACCESS = os.getenv('AWS_ACCESS_KEY_ID')
        AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if BUCKET_NAME:
            print(f"Uploading {output_path} to S3...")
            loader = S3Loader(BUCKET_NAME, AWS_ACCESS, AWS_SECRET)
            loader.upload_file(output_path, output_path) # Upload to root explicitly as requested by 'public_assets or root'
            # Or usually 'public_assets/dashboard_feed.csv' but app.py looks for 'dashboard_feed.csv' directly?
            # app.py download_file(bucket, 'dashboard_feed.csv', 'dashboard_feed.csv') implies ROOT.
            # So we upload to ROOT.

    finally:
        conn.close()

if __name__ == "__main__":
    generate_dashboard_feed()
