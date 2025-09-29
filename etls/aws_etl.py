from praw.models import TrophyList
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY, AWS_BUCKET_NAME, AWS_REGION
import boto3
from botocore.exceptions import ClientError

def connect_to_s3():
    try:
        print(f"Connecting to S3 with key: {AWS_ACCESS_KEY_ID[:10]}...")
        print(f"Using bucket: {AWS_BUCKET_NAME}")
        print(f"Using region: {AWS_REGION}")
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        print("S3 connection established successfully")
        return s3_client
    except Exception as e:
        print(f"ERROR connecting to S3: {e}")
        return None


def create_bucket_if_not_exist(s3_client, bucket_name: str):
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"Bucket {bucket_name} does not exist. Please create it manually in AWS Console.")
            raise Exception(f"Bucket {bucket_name} not found. Please create it first.")
        else:
            print(f"Error checking bucket: {e}")
            raise e

def upload_to_s3(s3_client, file_name: str, bucket: str, s3_file_name: str):
    try:
        print(f"Attempting to upload file: {file_name}")
        print(f"Target S3 path: s3://{bucket}/raw/{s3_file_name}")
        
        # Check if local file exists
        import os
        if not os.path.exists(file_name):
            print(f"ERROR: Local file {file_name} does not exist!")
            return False
            
        # Upload file using boto3
        s3_key = f"raw/{s3_file_name}"
        s3_client.upload_file(file_name, bucket, s3_key)
        print(f"File uploaded successfully to s3://{bucket}/{s3_key}")
        return True
    except ClientError as e:
        print(f"ERROR uploading to S3: {e}")
        print(f"Error Code: {e.response['Error']['Code']}")
        print(f"Error Message: {e.response['Error']['Message']}")
        return False
    except Exception as e:
        print(f"ERROR uploading to S3: {e}")
        return False
        
        