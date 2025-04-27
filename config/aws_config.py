# config/aws_config.py

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS Credentials and Region
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

# S3 Configuration
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'financial-data-analytics')
S3_RAW_DATA_PREFIX = 'raw/'
S3_PROCESSED_DATA_PREFIX = 'processed/'
S3_ARCHIVE_PREFIX = 'archive/'

# S3 Storage Classes and Lifecycle Configuration
S3_STORAGE_CLASSES = {
    'HOT_DATA': 'STANDARD',            # For frequently accessed data
    'WARM_DATA': 'STANDARD_IA',        # For less frequently accessed data
    'COLD_DATA': 'GLACIER',            # For rarely accessed archival data
}

# Lifecycle transition days 
TRANSITION_TO_IA_DAYS = 30             # Move to STANDARD_IA after 30 days
TRANSITION_TO_GLACIER_DAYS = 90        # Move to GLACIER after 90 days
EXPIRATION_DAYS = 365                  # Delete data after 1 year

# CloudWatch Configuration
CLOUDWATCH_NAMESPACE = 'FinancialDataAnalytics'
METRIC_DIMENSIONS = {
    'Environment': os.getenv('ENVIRONMENT', 'development'),
    'Application': 'FinancialDataPipeline'
}