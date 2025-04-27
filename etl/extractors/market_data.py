# etl/extractors/market_data.py

import os
import logging
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta
from .base_extractor import BaseExtractor

logger = logging.getLogger(__name__)

class MarketDataExtractor(BaseExtractor):
    """
    Extractor for market data from various sources.
    
    This extractor can pull market data from:
    - Yahoo Finance API (via yfinance)
    - CSV files
    - Custom APIs
    """
    
    def __init__(self, source_name, config=None):
        """
        Initialize the market data extractor.
        
        Args:
            source_name (str): Name of the data source
            config (dict, optional): Configuration parameters for the extractor
        """
        super().__init__(source_name, config)
        self.data_source_type = self.config.get('data_source_type', 'yahoo_finance')
    
    def extract(self, **kwargs):
        """
        Extract market data from the configured source.
        
        Args:
            **kwargs: Additional parameters for extraction
            
        Returns:
            pandas.DataFrame: Extracted market data
        """
        logger.info(f"Extracting market data from {self.source_name} using {self.data_source_type}")
        
        if self.data_source_type == 'yahoo_finance':
            return self._extract_from_yahoo_finance(**kwargs)
        elif self.data_source_type == 'csv':
            return self._extract_from_csv(**kwargs)
        elif self.data_source_type == 'api':
            return self._extract_from_api(**kwargs)
        else:
            logger.error(f"Unsupported data source type: {self.data_source_type}")
            return pd.DataFrame()
    
    def _extract_from_yahoo_finance(self, **kwargs):
        """
        Extract market data from Yahoo Finance API.
        
        Args:
            symbols (list): List of ticker symbols to extract
            start_date (str): Start date for data extraction (YYYY-MM-DD)
            end_date (str): End date for data extraction (YYYY-MM-DD)
            interval (str): Data interval ('1d', '1wk', '1mo', etc.)
            
        Returns:
            pandas.DataFrame: Market data
        """
        symbols = kwargs.get('symbols', self.config.get('symbols', ['SPY', 'AAPL', 'MSFT']))
        start_date = kwargs.get('start_date', self.config.get('start_date', 
                               (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')))
        end_date = kwargs.get('end_date', self.config.get('end_date', 
                             datetime.now().strftime('%Y-%m-%d')))
        interval = kwargs.get('interval', self.config.get('interval', '1d'))
        
        try:
            if isinstance(symbols, list) and len(symbols) > 1:
                # Multiple symbols
                data = yf.download(
                    tickers=symbols,
                    start=start_date,
                    end=end_date,
                    interval=interval,
                    group_by='ticker',
                    auto_adjust=True,
                    threads=True
                )
                
                # Restructure multi-index DataFrame
                if isinstance(data.columns, pd.MultiIndex):
                    # Create a list to store data for each symbol
                    dfs = []
                    
                    for symbol in symbols:
                        if symbol in data.columns:
                            # Extract data for this symbol
                            symbol_data = data[symbol].copy()
                            # Add symbol column
                            symbol_data['Symbol'] = symbol
                            # Reset index to make Date a regular column
                            symbol_data.reset_index(inplace=True)
                            dfs.append(symbol_data)
                    
                    # Concatenate all symbol data
                    if dfs:
                        combined_data = pd.concat(dfs, ignore_index=True)
                        return combined_data
                    else:
                        logger.warning("No data found for any symbols")
                        return pd.DataFrame()
                else:
                    # Single symbol result
                    data['Symbol'] = symbols[0]
                    data.reset_index(inplace=True)
                    return data
            else:
                # Single symbol
                symbol = symbols[0] if isinstance(symbols, list) else symbols
                data = yf.download(
                    tickers=symbol,
                    start=start_date,
                    end=end_date,
                    interval=interval,
                    auto_adjust=True
                )
                data['Symbol'] = symbol
                data.reset_index(inplace=True)
                return data
                
        except Exception as e:
            logger.error(f"Error extracting data from Yahoo Finance: {str(e)}")
            return pd.DataFrame()
    
    def _extract_from_csv(self, **kwargs):
        """
        Extract market data from CSV file.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            pandas.DataFrame: Market data
        """
        file_path = kwargs.get('file_path', self.config.get('file_path'))
        
        if not file_path or not os.path.exists(file_path):
            logger.error(f"CSV file not found: {file_path}")
            return pd.DataFrame()
        
        try:
            data = pd.read_csv(file_path)
            # Convert date column to datetime if it exists
            date_column = kwargs.get('date_column', self.config.get('date_column', 'Date'))
            if date_column in data.columns:
                data[date_column] = pd.to_datetime(data[date_column], errors='coerce')
            
            return data
        except Exception as e:
            logger.error(f"Error extracting data from CSV file: {str(e)}")
            return pd.DataFrame()
    
    def _extract_from_api(self, **kwargs):
        """
        Extract market data from a custom API.
        
        Args:
            api_url (str): URL of the API
            params (dict): Parameters for the API request
            
        Returns:
            pandas.DataFrame: Market data
        """
        api_url = kwargs.get('api_url', self.config.get('api_url'))
        params = kwargs.get('params', self.config.get('params', {}))
        headers = kwargs.get('headers', self.config.get('headers', {}))
        
        if not api_url:
            logger.error("API URL not provided")
            return pd.DataFrame()
        
        try:
            response = requests.get(api_url, params=params, headers=headers)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            # Parse JSON response
            json_data = response.json()
            
            # Extract data based on the JSON structure
            # This might need customization based on the API response format
            data_key = kwargs.get('data_key', self.config.get('data_key'))
            if data_key and data_key in json_data:
                data = pd.DataFrame(json_data[data_key])
            else:
                data = pd.DataFrame(json_data)
            
            return data
        except Exception as e:
            logger.error(f"Error extracting data from API: {str(e)}")
            return pd.DataFrame()