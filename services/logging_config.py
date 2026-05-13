"""
F1 Race Intelligence Lakehouse - Logging Configuration
Enterprise-grade logging setup for production analytics platform
"""

import logging
import sys
from datetime import datetime
from typing import Optional
import os

class F1Logger:
    """
    Enterprise logger for F1 Race Intelligence Lakehouse
    Provides structured logging with multiple output formats
    """
    
    def __init__(self, name: str = "F1-Race-Intelligence", level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Setup formatters
        self._setup_formatters()
        self._setup_handlers()
    
    def _setup_formatters(self):
        """Setup different log formatters for various outputs"""
        
        # Detailed formatter for file logging
        self.detailed_formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)-25s | %(funcName)-20s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Simple formatter for console
        self.console_formatter = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # JSON formatter for structured logging
        self.json_formatter = logging.Formatter(
            fmt='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "function": "%(funcName)s", "message": "%(message)s"}',
            datefmt='%Y-%m-%dT%H:%M:%S'
        )
    
    def _setup_handlers(self):
        """Setup console and file handlers"""
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self.console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler for general logs
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        file_handler = logging.FileHandler(
            f"{log_dir}/f1_lakehouse_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler.setFormatter(self.detailed_formatter)
        self.logger.addHandler(file_handler)
        
        # Error file handler
        error_handler = logging.FileHandler(
            f"{log_dir}/f1_lakehouse_errors_{datetime.now().strftime('%Y%m%d')}.log"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(self.detailed_formatter)
        self.logger.addHandler(error_handler)
        
        # JSON structured log handler
        json_handler = logging.FileHandler(
            f"{log_dir}/f1_lakehouse_structured_{datetime.now().strftime('%Y%m%d')}.json"
        )
        json_handler.setFormatter(self.json_formatter)
        self.logger.addHandler(json_handler)
    
    def get_logger(self):
        """Get the configured logger instance"""
        return self.logger

def setup_logging(logger_name: str = "F1-Race-Intelligence", level: str = "INFO") -> logging.Logger:
    """
    Convenience function to setup logging
    
    Args:
        logger_name: Name of the logger
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Configured logger instance
    """
    f1_logger = F1Logger(logger_name, level)
    return f1_logger.get_logger()

def log_pipeline_start(pipeline_name: str, logger: logging.Logger):
    """Log pipeline start with metadata"""
    logger.info("=" * 80)
    logger.info(f"STARTING PIPELINE: {pipeline_name}")
    logger.info(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)

def log_pipeline_end(pipeline_name: str, logger: logging.Logger, status: str = "SUCCESS"):
    """Log pipeline end with metadata"""
    logger.info("=" * 80)
    logger.info(f"COMPLETED PIPELINE: {pipeline_name}")
    logger.info(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Status: {status}")
    logger.info("=" * 80)

def log_data_metrics(table_name: str, row_count: int, columns: list, logger: logging.Logger):
    """Log data processing metrics"""
    logger.info(f"TABLE METRICS - {table_name}:")
    logger.info(f"  Rows: {row_count:,}")
    logger.info(f"  Columns: {len(columns)}")
    logger.info(f"  Column Names: {', '.join(columns[:10])}{'...' if len(columns) > 10 else ''}")

def log_quality_results(results: list, logger: logging.Logger):
    """Log data quality check results"""
    if not results:
        return
    
    passed = len([r for r in results if r.status == 'PASS'])
    failed = len([r for r in results if r.status == 'FAIL'])
    warnings = len([r for r in results if r.status == 'WARNING'])
    
    logger.info(f"DATA QUALITY SUMMARY:")
    logger.info(f"  Total Checks: {len(results)}")
    logger.info(f"  Passed: {passed}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Warnings: {warnings}")
    
    if failed > 0:
        logger.error("FAILED CHECKS:")
        for result in results:
            if result.status == 'FAIL':
                logger.error(f"  - {result.check_name}: {result.error_message}")

# Context manager for logging pipeline execution
class PipelineLogger:
    """Context manager for pipeline execution logging"""
    
    def __init__(self, pipeline_name: str, logger: Optional[logging.Logger] = None):
        self.pipeline_name = pipeline_name
        self.logger = logger or setup_logging()
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        log_pipeline_start(self.pipeline_name, self.logger)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        if exc_type is None:
            log_pipeline_end(self.pipeline_name, self.logger, "SUCCESS")
            self.logger.info(f"Pipeline Duration: {duration.total_seconds():.2f} seconds")
        else:
            log_pipeline_end(self.pipeline_name, self.logger, "FAILED")
            self.logger.error(f"Pipeline Duration: {duration.total_seconds():.2f} seconds")
            self.logger.error(f"Error: {exc_val}")

# Global logger instance
global_logger = setup_logging()
