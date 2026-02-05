"""
Data extraction modules.

Provides functionality to extract data from various sources including
files (JSON, CSV, Parquet) with support for incremental extraction.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Base class for data extractors."""
    
    def extract(self, **kwargs) -> pd.DataFrame:
        """Extract data from source."""
        raise NotImplementedError


class FileExtractor(DataExtractor):
    """
    Extract data from JSON/CSV/Parquet files.
    
    This class handles reading data from various file formats and converting
    them into pandas DataFrames, which are the standard data structure used
    throughout the pipeline for data manipulation.
    """
    
    def __init__(self, file_path: str):
        """
        Initialize extractor with file path.
        
        Args:
            file_path: Path to the source data file
        """
        self.file_path = Path(file_path)
    
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract data from file into a pandas DataFrame.
        
        This method:
        1. Checks if the file exists
        2. Determines the file format (JSON, CSV, or Parquet)
        3. Reads the file using the appropriate method
        4. Handles common errors (empty files, malformed JSON, etc.)
        5. Returns a DataFrame ready for transformation
        
        Args:
            **kwargs: Additional extraction parameters (not currently used)
        
        Returns:
            DataFrame containing the extracted data, or empty DataFrame if file not found
        
        Raises:
            ValueError: If file format is unsupported or JSON is malformed
        """
        logger.info(f"Extracting data from {self.file_path}")
        
        # Check if file exists before attempting to read
        if not self.file_path.exists():
            logger.warning(f"File not found: {self.file_path}")
            return pd.DataFrame()  # Return empty DataFrame instead of crashing
        
        # Determine file type and read accordingly
        # Different file formats require different pandas read functions
        if self.file_path.suffix == '.json':
            try:
                # Read JSON file with UTF-8 encoding to handle special characters
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)  # Parse JSON string into Python objects
                
                # Handle edge cases: empty files or non-list data structures
                if not data:
                    logger.warning(f"Empty JSON file: {self.file_path}")
                    return pd.DataFrame()
                
                # pandas DataFrame expects a list of dictionaries
                # If we get a single dict, wrap it in a list
                # If we get something else, return empty list
                if not isinstance(data, list):
                    logger.warning(f"JSON data is not a list, wrapping: {self.file_path}")
                    data = [data] if isinstance(data, dict) else []
                
                # Convert list of dictionaries to DataFrame
                # Each dictionary becomes a row, keys become columns
                df = pd.DataFrame(data)
                
            except json.JSONDecodeError as e:
                # JSON syntax error - file is corrupted or not valid JSON
                logger.error(f"Invalid JSON in file {self.file_path}: {e}")
                raise ValueError(f"Invalid JSON format in {self.file_path}: {e}") from e
            except Exception as e:
                # Any other error (permissions, disk issues, etc.)
                logger.error(f"Error reading JSON file {self.file_path}: {e}")
                raise
                
        elif self.file_path.suffix == '.csv':
            # CSV files can be read directly with pandas
            df = pd.read_csv(self.file_path)
            
        elif self.file_path.suffix == '.parquet':
            # Parquet is a columnar format, efficient for large datasets
            df = pd.read_parquet(self.file_path)
            
        else:
            # Unsupported format - fail fast with clear error message
            raise ValueError(f"Unsupported file format: {self.file_path.suffix}")
        
        # Log success with record count for monitoring
        logger.info(f"Extracted {len(df)} records from {self.file_path}")
        return df


class IncrementalExtractor(FileExtractor):
    """Extract data incrementally based on timestamp."""
    
    def extract(self, since: Optional[datetime] = None, **kwargs) -> pd.DataFrame:
        """
        Extract data since a given timestamp.
        
        Args:
            since: Extract records after this timestamp
            **kwargs: Additional extraction parameters
        
        Returns:
            DataFrame with extracted data
        """
        df = super().extract(**kwargs)
        
        if since and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df[df['timestamp'] > since]
            logger.info(f"Filtered to {len(df)} records since {since}")
        
        return df


def extract_user_events(data_path: str, since: Optional[datetime] = None) -> pd.DataFrame:
    """
    Extract user events data.
    
    Args:
        data_path: Path to user events data file
        since: Optional timestamp to extract incremental data
    
    Returns:
        DataFrame with user events
    """
    extractor = IncrementalExtractor(data_path)
    return extractor.extract(since=since)


def extract_subscriptions(data_path: str, since: Optional[datetime] = None) -> pd.DataFrame:
    """
    Extract subscription data.
    
    Args:
        data_path: Path to subscriptions data file
        since: Optional timestamp to extract incremental data
    
    Returns:
        DataFrame with subscriptions
    """
    extractor = IncrementalExtractor(data_path)
    return extractor.extract(since=since)


def extract_transactions(data_path: str, since: Optional[datetime] = None) -> pd.DataFrame:
    """
    Extract transaction data.
    
    Args:
        data_path: Path to transactions data file
        since: Optional timestamp to extract incremental data
    
    Returns:
        DataFrame with transactions
    """
    extractor = IncrementalExtractor(data_path)
    return extractor.extract(since=since)


def extract_user_profiles(data_path: str) -> pd.DataFrame:
    """
    Extract user profile data.
    
    Args:
        data_path: Path to user profiles data file
    
    Returns:
        DataFrame with user profiles
    """
    extractor = FileExtractor(data_path)
    return extractor.extract()
