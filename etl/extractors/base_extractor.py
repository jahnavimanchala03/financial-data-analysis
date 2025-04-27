# etl/extractors/base_extractor.py

import logging
from abc import ABC, abstractmethod
import pandas as pd
import os

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """
    Abstract base class for all data extractors.
    
    This class provides a common interface for all data extractors
    used in the ETL pipeline. Specific extractors should inherit from
    this class and implement the abstract methods.
    """
    
    def __init__(self, source_name, config=None):
        """
        Initialize the extractor.
        
        Args:
            source_name (str): Name of the data source
            config (dict, optional): Configuration parameters for the extractor
        """
        self.source_name = source_name
        self.config = config or {}
        logger.info(f"Initialized {self.__class__.__name__} for source: {source_name}")
    
    @abstractmethod
    def extract(self, **kwargs):
        """
        Extract data from the source.
        
        This method must be implemented by subclasses to extract data
        from the specific source.
        
        Returns:
            pandas.DataFrame or dict: The extracted data
        """
        pass
    
    def validate_extraction(self, data):
        """
        Perform basic validation on extracted data.
        
        Args:
            data: The extracted data to validate
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        if isinstance(data, pd.DataFrame):
            if data.empty:
                logger.warning(f"Extracted empty DataFrame from {self.source_name}")
                return False
            logger.info(f"Extracted {len(data)} rows from {self.source_name}")
            return True
        elif isinstance(data, dict):
            if not data:
                logger.warning(f"Extracted empty dictionary from {self.source_name}")
                return False
            logger.info(f"Extracted dictionary with {len(data)} keys from {self.source_name}")
            return True
        else:
            logger.warning(f"Unknown data type extracted from {self.source_name}: {type(data)}")
            return False
    
    def save_raw_data(self, data, output_path):
        """
        Save raw extracted data to a file.
        
        Args:
            data: The extracted data
            output_path (str): Path where to save the data
            
        Returns:
            str: Path to the saved file or None if failed
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            if isinstance(data, pd.DataFrame):
                # Save DataFrame to CSV
                data.to_csv(output_path, index=False)
                logger.info(f"Saved raw data to {output_path}")
                return output_path
            else:
                # For other data types, might need to implement specific saving logic
                logger.warning(f"Saving data of type {type(data)} not implemented")
                return None
        except Exception as e:
            logger.error(f"Error saving raw data: {str(e)}")
            return None
    
    def log_extraction_stats(self, data):
        """
        Log statistics about the extracted data.
        
        Args:
            data: The extracted data
        """
        if isinstance(data, pd.DataFrame):
            logger.info(f"Extraction stats for {self.source_name}:")
            logger.info(f"  - Shape: {data.shape}")
            logger.info(f"  - Columns: {list(data.columns)}")
            logger.info(f"  - Missing values: {data.isnull().sum().sum()}")
        else:
            logger.info(f"Extraction stats not available for data type: {type(data)}")