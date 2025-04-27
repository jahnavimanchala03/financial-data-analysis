# etl/transformers/data_cleaner.py

import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataCleaner:
    """
    Class for cleaning and transforming market data.
    
    This class provides methods to clean and transform raw market data,
    including handling missing values, outliers, and feature engineering.
    """
    
    def __init__(self, config=None):
        """
        Initialize the data cleaner.
        
        Args:
            config (dict, optional): Configuration parameters for cleaning
        """
        self.config = config or {}
        logger.info("Initialized DataCleaner")
    
    def clean_market_data(self, df):
        """
        Clean market data DataFrame.
        
        Args:
            df (pandas.DataFrame): Raw market data
            
        Returns:
            pandas.DataFrame: Cleaned market data
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for cleaning")
            return df
        
        logger.info(f"Cleaning market data with shape {df.shape}")
        
        # Create a copy of the DataFrame to avoid modifying the original
        cleaned_df = df.copy()
        
        # Apply cleaning operations
        cleaned_df = self._handle_missing_values(cleaned_df)
        cleaned_df = self._handle_outliers(cleaned_df)
        cleaned_df = self._standardize_column_names(cleaned_df)
        cleaned_df = self._convert_data_types(cleaned_df)
        
        logger.info(f"Cleaned data shape: {cleaned_df.shape}")
        return cleaned_df
    
    def _handle_missing_values(self, df):
        """
        Handle missing values in the DataFrame.
        
        Args:
            df (pandas.DataFrame): DataFrame with potential missing values
            
        Returns:
            pandas.DataFrame: DataFrame with handled missing values
        """
        # Count missing values before handling
        missing_before = df.isnull().sum().sum()
        
        # Handle missing values based on column type
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        # For numeric columns, use forward fill then backward fill
        # This is common for time series financial data
        if len(numeric_cols) > 0:
            df[numeric_cols] = df[numeric_cols].fillna(method='ffill')
            df[numeric_cols] = df[numeric_cols].fillna(method='bfill')
            
            # If still missing, use median
            for col in numeric_cols:
                if df[col].isnull().any():
                    median_val = df[col].median()
                    df[col] = df[col].fillna(median_val)
                    logger.info(f"Filled missing values in {col} with median: {median_val}")
        
        # For categorical/object columns, fill with mode
        cat_cols = df.select_dtypes(include=['object', 'category']).columns
        for col in cat_cols:
            if df[col].isnull().any():
                mode_val = df[col].mode()[0]
                df[col] = df[col].fillna(mode_val)
                logger.info(f"Filled missing values in {col} with mode: {mode_val}")
        
        # Count missing values after handling
        missing_after = df.isnull().sum().sum()
        logger.info(f"Handled missing values: {missing_before} before, {missing_after} after")
        
        return df
    
    def _handle_outliers(self, df):
        """
        Handle outliers in numeric columns.
        
        By default, uses IQR method to cap outliers at the boundaries.
        
        Args:
            df (pandas.DataFrame): DataFrame with potential outliers
            
        Returns:
            pandas.DataFrame: DataFrame with handled outliers
        """
        # Only process numeric columns that aren't date/time related
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        date_related_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        cols_to_process = [col for col in numeric_cols if col not in date_related_cols]
        
        outlier_method = self.config.get('outlier_method', 'iqr')
        
        if outlier_method == 'iqr':
            # IQR (Interquartile Range) method
            for col in cols_to_process:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Count outliers before capping
                outliers_before = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
                
                if outliers_before > 0:
                    # Cap the outliers
                    df[col] = np.where(df[col] < lower_bound, lower_bound, df[col])
                    df[col] = np.where(df[col] > upper_bound, upper_bound, df[col])
                    logger.info(f"Capped {outliers_before} outliers in {col} using IQR method")
        
        elif outlier_method == 'zscore':
            # Z-score method
            for col in cols_to_process:
                mean = df[col].mean()
                std = df[col].std()
                threshold = self.config.get('zscore_threshold', 3)
                
                # Count outliers before capping
                z_scores = (df[col] - mean) / std
                outliers_before = (abs(z_scores) > threshold).sum()
                
                if outliers_before > 0:
                    # Cap the outliers
                    lower_bound = mean - threshold * std
                    upper_bound = mean + threshold * std
                    
                    df[col] = np.where(df[col] < lower_bound, lower_bound, df[col])
                    df[col] = np.where(df[col] > upper_bound, upper_bound, df[col])
                    logger.info(f"Capped {outliers_before} outliers in {col} using Z-score method")
        
        return df
    
    def _standardize_column_names(self, df):
        """
        Standardize column names to a consistent format.
        
        Args:
            df (pandas.DataFrame): DataFrame with original column names
            
        Returns:
            pandas.DataFrame: DataFrame with standardized column names
        """
        # Convert column names to snake_case
        df.columns = [self._to_snake_case(col) for col in df.columns]
        
        # Standardize common financial column names
        rename_map = {
            'adj_close': 'adjusted_close',
            'adj_open': 'adjusted_open',
            'adj_high': 'adjusted_high',
            'adj_low': 'adjusted_low',
            'adj_volume': 'adjusted_volume',
            'symbol': 'ticker',
            'ticker_symbol': 'ticker',
            'stock': 'ticker',
            'last_price': 'close',
            'last': 'close'
        }
        
        # Only rename columns that exist
        cols_to_rename = {k: v for k, v in rename_map.items() if k in df.columns}
        if cols_to_rename:
            df = df.rename(columns=cols_to_rename)
            logger.info(f"Renamed columns: {cols_to_rename}")
        
        return df
    
    def _to_snake_case(self, name):
        """Convert a column name to snake_case."""
        import re
        # Replace spaces and special characters with underscores
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        # Replace multiple underscores with a single one
        s3 = re.sub('__+', '_', s2)
        # Replace any remaining special characters with underscores
        s4 = re.sub('[^0-9a-zA-Z]+', '_', s3)
        # Remove leading/trailing underscores
        return s4.strip('_')
    
    def _convert_data_types(self, df):
        """
        Convert columns to appropriate data types.
        
        Args:
            df (pandas.DataFrame): DataFrame with original data types
            
        Returns:
            pandas.DataFrame: DataFrame with converted data types
        """
        # Copy the DataFrame to avoid warnings about setting values on a copy
        df = df.copy()
        
        # Convert date columns to datetime
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    logger.info(f"Converted {col} to datetime")
                except Exception as e:
                    logger.warning(f"Failed to convert {col} to datetime: {str(e)}")
        
        # Convert known numeric columns to float
        price_columns = [col for col in df.columns if any(price_term in col.lower() 
                                                          for price_term in ['price', 'open', 'high', 'low', 'close', 'adj', 'vwap'])]
        for col in price_columns:
            if col in df.columns and df[col].dtype != 'float64':
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    logger.info(f"Converted {col} to numeric")
                except Exception as e:
                    logger.warning(f"Failed to convert {col} to numeric: {str(e)}")
        
        # Convert volume columns to int
        volume_columns = [col for col in df.columns if 'volume' in col.lower()]
        for col in volume_columns:
            if col in df.columns and df[col].dtype != 'int64':
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
                    logger.info(f"Converted {col} to integer")
                except Exception as e:
                    logger.warning(f"Failed to convert {col} to integer: {str(e)}")
        
        return df
    
    def engineer_features(self, df):
        """
        Add engineered features to the DataFrame.
        
        Args:
            df (pandas.DataFrame): Market data DataFrame
            
        Returns:
            pandas.DataFrame: DataFrame with additional features
        """
        logger.info("Engineering additional features")
        
        if df.empty:
            return df
        
        # Create a copy to avoid modifying the original
        df_features = df.copy()
        
        # Check if we have the necessary price columns
        price_cols = ['open', 'high', 'low', 'close']
        if not all(col in df_features.columns for col in price_cols):
            logger.warning("Missing required price columns for feature engineering")
            return df_features
        
        try:
            # Daily Returns
            df_features['daily_return'] = df_features.groupby('ticker')['close'].pct_change()
            
            # Volatility (20-day rolling standard deviation of returns)
            df_features['volatility_20d'] = df_features.groupby('ticker')['daily_return'].transform(
                lambda x: x.rolling(window=20).std()
            )
            
            # Moving Averages
            df_features['ma_5d'] = df_features.groupby('ticker')['close'].transform(
                lambda x: x.rolling(window=5).mean()
            )
            df_features['ma_20d'] = df_features.groupby('ticker')['close'].transform(
                lambda x: x.rolling(window=20).mean()
            )
            
            # MACD components
            df_features['ema_12d'] = df_features.groupby('ticker')['close'].transform(
                lambda x: x.ewm(span=12, adjust=False).mean()
            )
            df_features['ema_26d'] = df_features.groupby('ticker')['close'].transform(
                lambda x: x.ewm(span=26, adjust=False).mean()
            )
            df_features['macd'] = df_features['ema_12d'] - df_features['ema_26d']
            df_features['macd_signal'] = df_features.groupby('ticker')['macd'].transform(
                lambda x: x.ewm(span=9, adjust=False).mean()
            )
            
            # Bollinger Bands
            df_features['bb_middle'] = df_features['ma_20d']
            df_features['bb_std'] = df_features.groupby('ticker')['close'].transform(
                lambda x: x.rolling(window=20).std()
            )
            df_features['bb_upper'] = df_features['bb_middle'] + (df_features['bb_std'] * 2)
            df_features['bb_lower'] = df_features['bb_middle'] - (df_features['bb_std'] * 2)
            
            logger.info(f"Added {len(df_features.columns) - len(df.columns)} engineered features")
            
        except Exception as e:
            logger.error(f"Error during feature engineering: {str(e)}")
        
        return df_features