from etls.aws_etl import connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import AWS_BUCKET_NAME


def upload_s3_pipeline(ti):
    file_path = ti.xcom_pull(task_ids='reddit_extraction', key='return_value')
    
    print(f"Received file path from previous task: {file_path}")
    
    s3_client = connect_to_s3()
    if s3_client is None:
        raise Exception("Failed to connect to S3")
        
    create_bucket_if_not_exist(s3_client, AWS_BUCKET_NAME)

    # Extract just the filename for S3 key
    s3_file_name = file_path.split('/')[-1]
    print(f"Uploading file: {file_path} to S3 as: {s3_file_name}")
    
    success = upload_to_s3(s3_client, file_path, AWS_BUCKET_NAME, s3_file_name)
    
    if success:
        print("S3 upload completed successfully!")
    else:
        print("S3 upload failed!")
        raise Exception("Failed to upload file to S3")