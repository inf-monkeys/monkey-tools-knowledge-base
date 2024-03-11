from vines_worker_sdk.oss import OSSClient

from src.config import config_data

s3_config = config_data.get('s3')
oss_client = OSSClient(
    aws_access_key_id=s3_config.get('accessKeyId'),
    aws_secret_access_key=s3_config.get('secretAccessKey'),
    endpoint_url=s3_config.get('endpoint'),
    region_name=s3_config.get('region'),
    bucket_name=s3_config.get('bucket'),
    base_url=s3_config.get('publicUrl'),
)
