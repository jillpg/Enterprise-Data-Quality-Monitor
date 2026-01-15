import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def clean_s3_landing():
    bucket_name = os.getenv('S3_BUCKET_NAME')
    prefix = os.getenv('S3_LANDING_PREFIX', 'landing')
    
    aws_access = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')

    if not bucket_name or not aws_access:
        print("Error: Missing AWS credentials or bucket name.")
        return

    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access,
        aws_secret_access_key=aws_secret
    )
    bucket = s3.Bucket(bucket_name)

    print(f"Cleaning s3://{bucket_name}/{prefix} ...")
    
    # Delete all objects with the prefix
    # Note: This is a destructive operation!
    bucket.objects.filter(Prefix=prefix).delete()
    
    print("✨ S3 Landing Zone Cleared!")

if __name__ == "__main__":
    confirm = input("Are you sure you want to delete ALL files in the S3 landing zone? (yes/no): ")
    if confirm.lower() == "yes":
        clean_s3_landing()
    else:
        print("Operation cancelled.")
