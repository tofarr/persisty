import boto3

_S3_CLIENT = None


def get_s3_client():
    global _S3_CLIENT
    s3_client = _S3_CLIENT
    if not s3_client:
        s3_client = _S3_CLIENT = boto3.client('s3')
    return s3_client
