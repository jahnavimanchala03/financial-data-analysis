# utils/s3_utils.py

import os
import boto3
import logging
from botocore.exceptions import ClientError
from config.aws_config import (
    AWS_ACCESS_KEY_ID, 
    AWS_SECRET_ACCESS_KEY, 
    AWS_REGION,
    S3_BUCKET_NAME,
    S3_RAW_DATA_PREFIX,
    S3_PROCESSED_DATA_PREFIX,
    S3_ARCHIVE_PREFIX,
    S3_STORAGE_CLASSES
)

logger = logging.getLogger(__name__)

def get_s3_client():
    """
    Create and return an S3 client using AWS credentials.
    
    Returns:
        boto3.client: Authenticated S3 client
    """
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        return s3_client
    except Exception as e:
        logger.error(f"Error creating S3 client: {str(e)}")
        raise

def create_bucket_if_not_exists(bucket_name=S3_BUCKET_NAME):
    """
    Create S3 bucket if it doesn't already exist.
    
    Args:
        bucket_name (str): Name of the S3 bucket
    
    Returns:
        bool: True if bucket exists or was created successfully, False otherwise
    """
    s3_client = get_s3_client()
    
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists")
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket doesn't exist, create it
            try:
                if AWS_REGION == 'us-east-1':
                    # Special case for us-east-1 region
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    # For other regions, specify the LocationConstraint
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                    )
                logger.info(f"Created bucket '{bucket_name}' in region '{AWS_REGION}'")
                return True
            except Exception as create_error:
                logger.error(f"Error creating bucket: {str(create_error)}")
                return False
        else:
            logger.error(f"Error checking bucket existence: {str(e)}")
            return False

def upload_file_to_s3(file_path, prefix=S3_RAW_DATA_PREFIX, storage_class=None):
    """
    Upload a file to S3 bucket with specified prefix and storage class.
    
    Args:
        file_path (str): Path to the local file
        prefix (str): S3 prefix (folder path)
        storage_class (str): S3 storage class (STANDARD, STANDARD_IA, etc.)
    
    Returns:
        dict: Response from S3 put_object operation or None if failed
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None
    
    storage_class = storage_class or S3_STORAGE_CLASSES['HOT_DATA']
    s3_client = get_s3_client()
    file_name = os.path.basename(file_path)
    s3_key = f"{prefix}{file_name}"
    
    try:
        with open(file_path, 'rb') as file_data:
            response = s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=file_data,
                StorageClass=storage_class
            )
        logger.info(f"Uploaded {file_path} to s3://{S3_BUCKET_NAME}/{s3_key}")
        return response
    except Exception as e:
        logger.error(f"Error uploading file to S3: {str(e)}")
        return None

def download_file_from_s3(s3_key, local_path):
    """
    Download a file from S3 to a local path.
    
    Args:
        s3_key (str): S3 object key
        local_path (str): Local destination path
    
    Returns:
        bool: True if download was successful, False otherwise
    """
    s3_client = get_s3_client()
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download the file
        s3_client.download_file(S3_BUCKET_NAME, s3_key, local_path)
        logger.info(f"Downloaded s3://{S3_BUCKET_NAME}/{s3_key} to {local_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading file from S3: {str(e)}")
        return False

def list_files_in_prefix(prefix):
    """
    List all files in a specific S3 prefix.
    
    Args:
        prefix (str): S3 prefix to list
    
    Returns:
        list: List of object keys in the prefix
    """
    s3_client = get_s3_client()
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix=prefix
        )
        
        if 'Contents' in response:
            return [item['Key'] for item in response['Contents']]
        return []
    except Exception as e:
        logger.error(f"Error listing files in S3 prefix {prefix}: {str(e)}")
        return []

def setup_s3_lifecycle_policy():
    """
    Set up S3 lifecycle policies for intelligent data tiering.
    
    Returns:
        bool: True if lifecycle policy was set successfully, False otherwise
    """
    from config.aws_config import (
        TRANSITION_TO_IA_DAYS,
        TRANSITION_TO_GLACIER_DAYS,
        EXPIRATION_DAYS
    )
    
    s3_client = get_s3_client()
    
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'Raw-Data-Lifecycle',
                'Status': 'Enabled',
                'Prefix': S3_RAW_DATA_PREFIX,
                'Transitions': [
                    {
                        'Days': TRANSITION_TO_IA_DAYS,
                        'StorageClass': 'STANDARD_IA'
                    },
                    {
                        'Days': TRANSITION_TO_GLACIER_DAYS,
                        'StorageClass': 'GLACIER'
                    }
                ]
            },
            {
                'ID': 'Processed-Data-Lifecycle',
                'Status': 'Enabled',
                'Prefix': S3_PROCESSED_DATA_PREFIX,
                'Transitions': [
                    {
                        'Days': TRANSITION_TO_IA_DAYS * 2,  # Keep processed data in STANDARD longer
                        'StorageClass': 'STANDARD_IA'
                    }
                ]
            },
            {
                'ID': 'Archive-Data-Expiration',
                'Status': 'Enabled',
                'Prefix': S3_ARCHIVE_PREFIX,
                'Expiration': {
                    'Days': EXPIRATION_DAYS
                }
            }
        ]
    }
    
    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=S3_BUCKET_NAME,
            LifecycleConfiguration=lifecycle_config
        )
        logger.info(f"Set up lifecycle policy for bucket '{S3_BUCKET_NAME}'")
        return True
    except Exception as e:
        logger.error(f"Error setting up S3 lifecycle policy: {str(e)}")
        return False