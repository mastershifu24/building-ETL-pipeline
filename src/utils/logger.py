"""
Logging utility for the ETL pipeline.

Provides structured logging with different log levels and formatting.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


def setup_logger(
    name: str = "etl_pipeline",
    log_level: str = "INFO",
    log_path: Optional[str] = None
) -> logging.Logger:
    """
    Set up a logger with file and console handlers.
    
    Configures logging with both console output (INFO level) and optional
    file output (DEBUG level) with appropriate formatting for each.
    
    Args:
        name: Logger name identifier
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_path: Optional path to directory for log files. If provided,
                  creates daily log files with format: etl_pipeline_YYYYMMDD.log
    
    Returns:
        Configured logging.Logger instance with handlers attached
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler (if log_path is provided)
    if log_path:
        log_dir = Path(log_path)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = log_dir / f"etl_pipeline_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger
