# main.py

import os
import logging
import argparse
import yaml
from datetime import datetime, timedelta
import pandas as pd
from validation.data_validator import DataValidator
from monitoring.cloudwatch_monitor import CloudWatchMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('financial_data_analytics.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Import project modules
from utils.s3_utils import create_bucket_if_not_exists, setup_s3_lifecycle_policy
from etl.extractors.market_data import MarketDataExtractor
from etl.transformers.data_cleaner import DataCleaner
from etl.loaders.s3_loader import S3Loader

class FinancialDataPipeline:
    """
    Main pipeline for financial data analytics.
    
    This class orchestrates the ETL process for financial data, including extraction,
    transformation, validation, loading, and monitoring.
    """
    
    def __init__(self, config_file=None):
        """
        Initialize the financial data pipeline.
        
        Args:
            config_file (str, optional): Path to configuration file
        """
        self.config = self.load_config(config_file)
        self.initialize_components()
        logger.info("Initialized Financial Data Pipeline")
    
    def load_config(self, config_file):
        """
        Load configuration from a YAML file.
        
        Args:
            config_file (str): Path to configuration file
            
        Returns:
            dict: Configuration parameters
        """
        default_config = {
            'environment': 'development',
            'symbols': ['SPY', 'AAPL', 'MSFT', 'AMZN', 'GOOGL'],
            'data_source_type': 'yahoo_finance',
            'start_date': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            'end_date': datetime.now().strftime('%Y-%m-%d'),
            'interval': '1d',
            'output_format': 'parquet',
            'enable_validation': False,  # Initially set to False until we implement validation
            'enable_monitoring': False,  # Initially set to False until we implement monitoring
            'cloud_resources': {
                'create_bucket': True,
                'setup_lifecycle': True
            }
        }
        
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    user_config = yaml.safe_load(f)
                
                # Merge configurations
                config = {**default_config, **user_config}
                logger.info(f"Loaded configuration from {config_file}")
                return config
            except Exception as e:
                logger.error(f"Error loading configuration from {config_file}: {str(e)}")
                return default_config
        else:
            logger.info("Using default configuration")
            return default_config
    
    def initialize_components(self):
        """
        Initialize pipeline components based on configuration.
        """
        # Initialize S3 storage
        if self.config.get('cloud_resources', {}).get('create_bucket', True):
            create_bucket_if_not_exists()
        
        if self.config.get('cloud_resources', {}).get('setup_lifecycle', True):
            setup_s3_lifecycle_policy()
        
        # Initialize extractors
        extractor_config = {
            'data_source_type': self.config.get('data_source_type', 'yahoo_finance'),
            'symbols': self.config.get('symbols', ['SPY']),
            'start_date': self.config.get('start_date'),
            'end_date': self.config.get('end_date'),
            'interval': self.config.get('interval', '1d')
        }
        self.extractor = MarketDataExtractor('market_data', extractor_config)
        
        # Initialize transformers
        transformer_config = {
            'outlier_method': self.config.get('outlier_method', 'iqr')
        }
        self.cleaner = DataCleaner(transformer_config)
        
        # Initialize loaders
        loader_config = {}
        self.loader = S3Loader(loader_config)
        
        # Initialize validators
        self.validator = None
        if self.config.get('enable_validation', True):
            validator_config = {}
            self.validator = DataValidator(validator_config)

        # Initialize monitoring
        self.monitor = None
        if self.config.get('enable_monitoring', True):
            monitor_config = {
                'dimensions': {
                    'Environment': self.config.get('environment', 'development'),
                    'Application': 'FinancialDataPipeline'
                }
            }
            self.monitor = CloudWatchMonitor(monitor_config)
    
    def run_pipeline(self):
        """
        Run the financial data pipeline.
        
        This method orchestrates the execution of the complete ETL process:
        1. Extract market data
        2. Transform and clean the data
        3. Load the data to S3
        
        Returns:
            bool: True if the pipeline executed successfully, False otherwise
        """
        start_time = datetime.now()
        success = False
        
        try:
            logger.info("Starting Financial Data Pipeline")
            
            # 1. Extract data
            logger.info("Extracting market data...")
            raw_data = self.extractor.extract()
            
            if raw_data.empty:
                raise ValueError("Extraction returned empty data")

            if self.monitor:
                self.monitor.put_record_count(process_name, len(raw_data), 'Extracted')
        
            
            # 2. Transform and clean data
            logger.info("Cleaning and transforming data...")
            cleaned_data = self.cleaner.clean_market_data(raw_data)
            enriched_data = self.cleaner.engineer_features(cleaned_data)

            # Log record count
            if self.monitor:
                self.monitor.put_record_count(process_name, len(enriched_data), 'Processed')
              

            # 3. Validate data quality
            data_quality_score = 100.0  # Default perfect score

            if self.validator:
                logger.info("Validating data quality...")
                validation_results = self.validator.validate_data(enriched_data)
    
                # Save validation results
                validation_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                validation_file = f"validation/results_{validation_timestamp}.json"
                self.validator.save_validation_results(validation_results, validation_file)
    
                # Check validation results
                data_quality_score = validation_results.get('quality_score', 0.0)
                logger.info(f"Data quality score: {data_quality_score}%")
    
                if self.monitor:
                    self.monitor.put_data_quality_score(process_name, data_quality_score)
            
                # Check against threshold
                validation_threshold = self.config.get('validation_threshold', 90.0)
                if data_quality_score < validation_threshold:
                    logger.warning(f"Data quality score ({data_quality_score}%) is below threshold ({validation_threshold}%)")

                    if self.monitor:
                        self.monitor.put_error_count(process_name, 1, 'QualityBelowThreshold')

            # 4. Load data to S3
            logger.info("Loading data to S3...")
            output_format = self.config.get('output_format', 'parquet')
            
            # Define partition columns if needed
            partition_cols = self.config.get('partition_by', None)
            if partition_cols == 'date':
                # Common case: partition by date
                # Ensure we have a date column
                date_columns = [col for col in enriched_data.columns if 'date' in col.lower()]
                if date_columns:
                    partition_cols = [date_columns[0]]
                else:
                    partition_cols = None
            
            # Perform loading
            file_name = f"market_data_{datetime.now().strftime('%Y%m%d')}.{output_format}"
            s3_uri = self.loader.load_dataframe_to_s3(
                enriched_data,
                file_name,
                format=output_format,
                partition_cols=partition_cols
            )
            
            if not s3_uri:
                raise ValueError("Failed to load data to S3")
            
            logger.info(f"Data loaded successfully to {s3_uri}")
            
            # Mark success
            success = True
            
            return {
                'success': success,
                'extracted_rows': len(raw_data),
                'processed_rows': len(enriched_data),
                'quality_score': data_quality_score,
                's3_uri': s3_uri
            }
        
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            
            return {
                'success': False,
                'error': str(e)
            }
        
        finally:
            # Record end time and execution duration
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Pipeline completed in {duration:.2f} seconds")

            # Log duration and success flag
            if self.monitor:
                self.monitor.put_execution_time(process_name, start_time, end_time)
                self.monitor.put_success_flag(process_name, success)
    
    def run_historical_data_backfill(self, start_date, end_date, interval='1d'):
        """
        Run a historical data backfill for the specified date range.
        
        Args:
            start_date (str): Start date (YYYY-MM-DD)
            end_date (str): End date (YYYY-MM-DD)
            interval (str): Data interval ('1d', '1wk', etc.)
            
        Returns:
            dict: Results of the backfill process
        """
        logger.info(f"Starting historical data backfill from {start_date} to {end_date}")
        
        # Override the extractor configuration for historical data
        self.extractor.config['start_date'] = start_date
        self.extractor.config['end_date'] = end_date
        self.extractor.config['interval'] = interval
        
        # Run the pipeline with the new configuration
        result = self.run_pipeline()
        
        return result

def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Financial Data Analytics Pipeline')
    
    parser.add_argument('--config', type=str, default='config.yaml',
                        help='Path to configuration file')
    
    parser.add_argument('--action', type=str, default='run',
                        choices=['run', 'backfill'],
                        help='Action to perform')
    
    parser.add_argument('--start-date', type=str,
                        help='Start date for historical data (YYYY-MM-DD)')
    
    parser.add_argument('--end-date', type=str,
                        help='End date for historical data (YYYY-MM-DD)')
    
    parser.add_argument('--interval', type=str, default='1d',
                        choices=['1d', '1wk', '1mo'],
                        help='Data interval')
    
    return parser.parse_args()

def main():
    """
    Main entry point for the application.
    """
    # Parse arguments
    args = parse_arguments()
    
    # Initialize pipeline
    pipeline = FinancialDataPipeline(args.config)
    
    # Perform the requested action
    if args.action == 'run':
        # Run the regular pipeline
        result = pipeline.run_pipeline()
        print(f"Pipeline execution result: {result}")
    
    elif args.action == 'backfill':
        # Validate dates
        if not args.start_date or not args.end_date:
            print("Error: start-date and end-date are required for backfill")
            return
        
        # Run historical data backfill
        result = pipeline.run_historical_data_backfill(
            args.start_date,
            args.end_date,
            args.interval
        )
        print(f"Backfill execution result: {result}")

if __name__ == "__main__":
    main()