import json
import boto3
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Triggered by S3 PUT events on the streaming/ prefix.
    Logs the name of each new file to CloudWatch.
    """
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        file_size = record['s3']['object']['size']

        logger.info(f"[FreshCart] New streaming file detected")
        logger.info(f"  Bucket : {bucket}")
        logger.info(f"  File   : {key}")
        logger.info(f"  Size   : {file_size} bytes")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed {len(event['Records'])} S3 event(s)")
    }