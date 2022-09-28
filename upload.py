import boto3
from botocore.errorfactory import ClientError
import logging

def get_s3_client():
    s3_client = boto3.client('s3')    
    return s3_client

def upload_file(file, bucket, content):
    s3_client = get_s3_client()
    logger = logging.getLogger('main')

    #upload the file to the s3 bucket location
    try:
        put_res = s3_client.put_object(Bucket=bucket, Key=file, Body=content)
        logger.info("File upload successfull")
        return put_res
    except ClientError as e:
        logger.exception("Error while uploading to S3")
        return None


    
