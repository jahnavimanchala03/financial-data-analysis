# etl/loaders/s3_loader.py

import os
import logging
import pandas as pd
from io import StringIO, BytesIO
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

from config.aws_config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_BUCKET_NAME,
    S3_PROCESSED_DATA_PREFIX,
    S3_STORAGE_CLASSES
)

logger = logging.getLogger(__name__)

class S3Loader:
    """
    Class for loading processed data to S3.
    
    This class provides methods to load processed data to S3 in various formats,
    including CSV, Parquet, and JSON.
    """
    
    def __init__(self, config=None):
        """
        Initialize the S3 loader.
        
        Args:
            config (dict, optional): Configuration parameters for the loader
        """
        self.config = config or {}
        self.bucket_name = self.config.get('bucket_name', S3_BUCKET_NAME)
        self.processed_data_prefix = self.config.get('processed_data_prefix', S3_PROCESSED_DATA_PREFIX)
        logger.info(f"Initialized S3Loader for bucket: {self.bucket_name}")
        
        # Create S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
    
    def load_dataframe_to_s3(self, df, file_name, format='csv', partition_cols=None):
        """
        Load a DataFrame to S3 in the specified format.
        
        Args:
            df (pandas.DataFrame): DataFrame to load
            file_name (str): Base file name
            format (str): Output format ('csv', 'parquet', or 'json')
            partition_cols (list, optional): Columns to partition by
            
        Returns:
            str: S3 URI of the uploaded file or None if failed
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for loading")
            return None
        
        # Generate a timestamped path
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_base = os.path.splitext(file_name)[0]
        
        try:
            if partition_cols and isinstance(partition_cols, list) and len(partition_cols) > 0:
                # Partitioned data (multiple files)
                return self._load_partitioned_data(df, file_base, timestamp, format, partition_cols)
            else:
                # Single file
                return self._load_single_file(df, file_base, timestamp, format)
        except Exception as e:
            logger.error(f"Error loading DataFrame to S3: {str(e)}")
            return None
    
    def _load_single_file(self, df, file_base, timestamp, format):
        """
        Load a single file to S3.
        
        Args:
            df (pandas.DataFrame): DataFrame to load
            file_base (str): Base file name
            timestamp (str): Timestamp string
            format (str): Output format
            
        Returns:
            str: S3 URI of the uploaded file or None if failed
        """
        if format.lower() == 'csv':
            return self._load_csv_to_s3(df, file_base, timestamp)
        elif format.lower() == 'parquet':
            return self._load_parquet_to_s3(df, file_base, timestamp)
        elif format.lower() == 'json':
            return self._load_json_to_s3(df, file_base, timestamp)
        else:
            logger.error(f"Unsupported format: {format}")
            return None
    
    def _load_csv_to_s3(self, df, file_base, timestamp):
        """
        Load DataFrame as CSV to S3.
        
        Args:
            df (pandas.DataFrame): DataFrame to load
            file_base (str): Base file name
            timestamp (str): Timestamp string
            
        Returns:
            str: S3 URI of the uploaded file or None if failed
        """
        try:
            # Create buffer and save DataFrame as CSV
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # Define S3 key (path)
            s3_key = f"{self.processed_data_prefix}{file_base}/{timestamp}.csv"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv',
                StorageClass=S3_STORAGE_CLASSES['HOT_DATA']
            )
            
            logger.info(f"Uploaded DataFrame with {len(df)} rows to s3://{self.bucket_name}/{s3_key}")
            return f"s3://{self.bucket_name}/{s3_key}"
        except Exception as e:
            logger.error(f"Error loading CSV to S3: {str(e)}")
            return None
    
    def _load_parquet_to_s3(self, df, file_base, timestamp):
        """
        Load DataFrame as Parquet to S3.
        
        Args:
            df (pandas.DataFrame): DataFrame to load
            file_base (str): Base file name
            timestamp (str): Timestamp string
            
        Returns:
            str: S3 URI of the uploaded file or None if failed
        """
        try:
            # Create buffer and save DataFrame as Parquet
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
            parquet_buffer.seek(0)
            
            # Define S3 key (path)
            s3_key = f"{self.processed_data_prefix}{file_base}/{timestamp}.parquet"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream',
                StorageClass=S3_STORAGE_CLASSES['HOT_DATA']
            )
            
            logger.info(f"Uploaded DataFrame with {len(df)} rows to s3://{self.bucket_name}/{s3_key}")
            return f"s3://{self.bucket_name}/{s3_key}"
        except Exception as e:
            logger.error(f"Error loading Parquet to S3: {str(e)}")
            return None
    
    def _load_json_to_s3(self, df, file_base, timestamp):
        """
        Load DataFrame as JSON to S3.
        
        Args:
            df (pandas.DataFrame): DataFrame to load
            file_base (str): Base file name
            timestamp (str): Timestamp string
            
        Returns:
            str: S3 URI of the uploaded file or None if failed
        """
        try:
            # Create buffer and save DataFrame as JSON
            json_buffer = StringIO()
            df.to_json(json_buffer, orient='records', lines=True)
            
            # Define S3 key (path)
            s3_key = f"{self.processed_data_prefix}{file_base}/{timestamp}.json"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_buffer.getvalue(),
                ContentType='application/json',
                StorageClass=S3_STORAGE_CLASSES['HOT_DATA']
            )
            
            logger.info(f"Uploaded DataFrame with {len(df)} rows to s3://{self.bucket_name}/{s3_key}")
            return f"s3://{self.bucket_name}/{s3_key}"
        except Exception as e:
            logger.error(f"Error loading JSON to S3: {str(e)}")
            return None
    
    def _load_partitioned_data(self, df, file_base, timestamp, format, partition_cols):
        """
        Load partitioned data to S3.
        
        Args:
            df (pandas.DataFrame): DataFrame to load
            file_base (str): Base file name
            timestamp (str): Timestamp string
            format (str): Output format
            partition_cols (list): Columns to partition by
            
        Returns:
            str: S3 URI of the parent directory or None if failed
        """
        try:
            # Create parent directory path
            parent_path = f"{self.processed_data_prefix}{file_base}/{timestamp}"
            s3_uris = []
            
            # Process each partition
            for partition_values, partition_df in df.groupby(partition_cols):
                # Handle both single and multiple partition columns
                if not isinstance(partition_values, tuple):
                    partition_values = (partition_values,)
                
                # Create partition path
                partition_path = "/".join([f"{col}={val}" for col, val in zip(partition_cols, partition_values)])
                partition_key = f"{parent_path}/{partition_path}/"
                
                # Generate file name for this partition
                partition_file_name = f"part_{len(s3_uris):05d}"
                
                # Load the partition based on format
                if format.lower() == 'csv':
                    csv_buffer = StringIO()
                    partition_df.to_csv(csv_buffer, index=False)
                    s3_key = f"{partition_key}{partition_file_name}.csv"
                    content_type = 'text/csv'
                    body = csv_buffer.getvalue()
                elif format.lower() == 'parquet':
                    parquet_buffer = BytesIO()
                    partition_df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
                    parquet_buffer.seek(0)
                    s3_key = f"{partition_key}{partition_file_name}.parquet"
                    content_type = 'application/octet-stream'
                    body = parquet_buffer.getvalue()
                elif format.lower() == 'json':
                    json_buffer = StringIO()
                    partition_df.to_json(json_buffer, orient='records', lines=True)
                    s3_key = f"{partition_key}{partition_file_name}.json"
                    content_type = 'application/json'
                    body = json_buffer.getvalue()
                else:
                    logger.error(f"Unsupported format: {format}")
                    return None
                
                # Upload to S3
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=body,
                    ContentType=content_type,
                    StorageClass=S3_STORAGE_CLASSES['HOT_DATA']
                )
                
                s3_uris.append(f"s3://{self.bucket_name}/{s3_key}")
                logger.info(f"Uploaded partition to s3://{self.bucket_name}/{s3_key}")
            
            logger.info(f"Uploaded {len(s3_uris)} partitions to S3")
            return f"s3://{self.bucket_name}/{parent_path}/"
        except Exception as e:
            logger.error(f"Error loading partitioned data to S3: {str(e)}")
            return None
    
    def list_files(self, prefix):
        """
        List files in a prefix.
        
        Args:
            prefix (str): S3 prefix to list
            
        Returns:
            list: List of S3 keys in the prefix
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                keys = [item['Key'] for item in response['Contents']]
                logger.info(f"Found {len(keys)} files in s3://{self.bucket_name}/{prefix}")
                return keys
            else:
                logger.info(f"No files found in s3://{self.bucket_name}/{prefix}")
                return []
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return []
    
    def download_file_from_s3(self, s3_key, local_path):
        """
        Download a file from S3.
        
        Args:
            s3_key (str): S3 object key
            local_path (str): Local destination path
            
        Returns:
            bool: True if download was successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download the file
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Downloaded s3://{self.bucket_name}/{s3_key} to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading file from S3: {str(e)}")
            return False