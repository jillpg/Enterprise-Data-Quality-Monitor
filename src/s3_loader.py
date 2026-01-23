import boto3
import os
from botocore.exceptions import NoCredentialsError

class S3Loader:
    def __init__(self, bucket_name, aws_access_key, aws_secret_key):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

    def upload_file(self, local_path, s3_key):
        """
        Uploads a file to S3.
        """
        try:
            print(f"  Uploading {local_path} to s3://{self.bucket_name}/{s3_key} ...")
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            print("  Upload Success.")
            return True
        except FileNotFoundError:
            print(f"  The file {local_path} was not found.")
            return False
        except NoCredentialsError:
            print("  Credentials not available.")
            return False
        except Exception as e:
            print(f"  An error occurred: {e}")
            return False

    def download_file(self, s3_key, local_path):
        """
        Downloads a file from S3.
        """
        try:
            print(f"  Downloading s3://{self.bucket_name}/{s3_key} to {local_path} ...")
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            print("  Download Success.")
            return True
        except Exception as e:
            print(f"  Download Failed: {e}")
            return False

