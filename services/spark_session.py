"""
F1 Race Intelligence Lakehouse - Spark Session Service
Enterprise-grade Spark session management for F1 analytics platform
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
from typing import Optional, Dict, Any

class F1SparkSessionManager:
    """
    Enterprise Spark session manager for F1 Race Intelligence Lakehouse
    Optimized for Delta Lake operations and production workloads
    """
    
    def __init__(self, app_name: str = "F1-Race-Intelligence-Lakehouse"):
        self.app_name = app_name
        self.spark_session: Optional[SparkSession] = None
        self.logger = logging.getLogger(__name__)
        
    def create_spark_session(self, config: Optional[Dict[str, Any]] = None) -> SparkSession:
        """
        Create and configure Spark session with Delta Lake support
        
        Args:
            config: Additional Spark configuration parameters
            
        Returns:
            Configured SparkSession instance
        """
        if self.spark_session:
            self.logger.warning("Spark session already exists. Returning existing session.")
            return self.spark_session
            
        builder = SparkSession.builder.appName(self.app_name)
        
        # Delta Lake configuration
        builder = builder.config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
                         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                         .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
                         .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        
        # Performance optimizations for F1 analytics
        builder = builder.config("spark.sql.adaptive.enabled", "true") \
                         .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                         .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Apply additional configuration if provided
        if config:
            for key, value in config.items():
                builder = builder.config(key, value)
        
        self.spark_session = builder.getOrCreate()
        
        # Set log level
        self.spark_session.sparkContext.setLogLevel("WARN")
        
        self.logger.info(f"Spark session '{self.app_name}' created successfully")
        return self.spark_session
    
    def get_spark_session(self) -> SparkSession:
        """Get existing Spark session or create new one"""
        if not self.spark_session:
            return self.create_spark_session()
        return self.spark_session
    
    def stop_spark_session(self):
        """Stop the Spark session"""
        if self.spark_session:
            self.spark_session.stop()
            self.spark_session = None
            self.logger.info("Spark session stopped")
    
    def __enter__(self):
        """Context manager entry"""
        return self.create_spark_session()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop_spark_session()

# Global session manager instance
spark_manager = F1SparkSessionManager()

def get_spark_session() -> SparkSession:
    """Convenience function to get Spark session"""
    return spark_manager.get_spark_session()
