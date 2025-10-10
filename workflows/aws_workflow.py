from data_collectors.aws_s3_client import connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import AWS_BUCKET_NAME
from datetime import datetime
import os


def upload_s3_pipeline(ti):
    file_path = ti.xcom_pull(task_ids='reddit_extraction', key='return_value')
    
    print(f"Received file path from previous task: {file_path}")
    
    s3_client = connect_to_s3()
    if s3_client is None:
        raise Exception("Failed to connect to S3")
        
    create_bucket_if_not_exist(s3_client, AWS_BUCKET_NAME)

    # Create partitioned S3 key based on current date
    current_date = datetime.now()
    year = current_date.year
    month = current_date.month
    day = current_date.day
    
    # S3 key with partitioning: raw/reddit/year=2024/month=01/day=15/
    s3_file_name = f"raw/reddit/year={year:04d}/month={month:02d}/day={day:02d}/reddit_data_{current_date.strftime('%Y%m%d_%H%M%S')}.csv"
    
    print(f"Uploading file: {file_path} to S3 as: {s3_file_name}")
    
    success = upload_to_s3(s3_client, file_path, AWS_BUCKET_NAME, s3_file_name)
    
    if success:
        print(f"S3 upload completed successfully! Partitioned key: {s3_file_name}")
        return s3_file_name
    else:
        print("S3 upload failed!")
        raise Exception("Failed to upload file to S3")