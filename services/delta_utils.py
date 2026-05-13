"""
F1 Race Intelligence Lakehouse - Delta Lake Utilities
Enterprise-grade Delta Lake operations for F1 analytics platform
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

class F1DeltaUtils:
    """
    Enterprise Delta Lake utilities for F1 Race Intelligence Lakehouse
    Provides optimized operations for medallion architecture
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
    
    def create_delta_table(self, 
                          df: DataFrame, 
                          path: str, 
                          table_name: str,
                          partition_by: Optional[List[str]] = None,
                          z_order_by: Optional[List[str]] = None) -> bool:
        """
        Create Delta table with enterprise optimizations
        
        Args:
            df: DataFrame to save
            path: Delta table path
            table_name: Table name for catalog registration
            partition_by: Partition columns
            z_order_by: Z-order columns for optimization
            
        Returns:
            Success status
        """
        try:
            writer = df.write.format("delta").mode("overwrite")
            
            if partition_by:
                writer = writer.partitionBy(partition_by)
            
            writer.save(path)
            
            # Register in catalog
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")
            
            # Apply Z-ordering if specified
            if z_order_by:
                delta_table = DeltaTable.forPath(self.spark, path)
                delta_table.optimize().executeZOrderBy(z_order_by)
            
            self.logger.info(f"Delta table {table_name} created successfully at {path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create Delta table {table_name}: {str(e)}")
            return False
    
    def upsert_to_delta(self, 
                       df: DataFrame, 
                       path: str, 
                       table_name: str,
                       merge_condition: str,
                       update_columns: Optional[List[str]] = None) -> bool:
        """
        Perform upsert (merge) operation on Delta table
        
        Args:
            df: DataFrame with new/updated data
            path: Delta table path
            table_name: Table name
            merge_condition: Merge condition for joining
            update_columns: Columns to update on match
            
        Returns:
            Success status
        """
        try:
            # Check if table exists
            if self._table_exists(path):
                delta_table = DeltaTable.forPath(self.spark, path)
                
                # Build merge operation
                merge_builder = delta_table.alias("target").merge(
                    df.alias("source"), 
                    merge_condition
                )
                
                # When matched, update specified columns
                if update_columns:
                    update_dict = {col: f"source.{col}" for col in update_columns}
                    merge_builder = merge_builder.whenMatchedUpdate(set=update_dict)
                else:
                    merge_builder = merge_builder.whenMatchedUpdateAll()
                
                # When not matched, insert all columns
                merge_builder = merge_builder.whenNotMatchedInsertAll()
                
                merge_builder.execute()
                
                self.logger.info(f"Upsert completed for table {table_name}")
            else:
                # Table doesn't exist, create it
                self.create_delta_table(df, path, table_name)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Upsert failed for table {table_name}: {str(e)}")
            return False
    
    def append_to_delta(self, 
                       df: DataFrame, 
                       path: str, 
                       table_name: str) -> bool:
        """
        Append data to Delta table
        
        Args:
            df: DataFrame to append
            path: Delta table path
            table_name: Table name
            
        Returns:
            Success status
        """
        try:
            df.write.format("delta").mode("append").save(path)
            
            # Register in catalog if not exists
            if not self._table_exists_in_catalog(table_name):
                self.spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")
            
            self.logger.info(f"Data appended to table {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Append failed for table {table_name}: {str(e)}")
            return False
    
    def optimize_delta_table(self, path: str, z_order_by: Optional[List[str]] = None) -> bool:
        """
        Optimize Delta table for performance
        
        Args:
            path: Delta table path
            z_order_by: Columns for Z-ordering
            
        Returns:
            Success status
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, path)
            
            # Compact small files
            delta_table.optimize().executeCompaction()
            
            # Apply Z-ordering if specified
            if z_order_by:
                delta_table.optimize().executeZOrderBy(z_order_by)
            
            self.logger.info(f"Delta table optimized at {path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Optimization failed for table at {path}: {str(e)}")
            return False
    
    def vacuum_delta_table(self, path: str, retention_hours: int = 168) -> bool:
        """
        Vacuum Delta table to remove old files
        
        Args:
            path: Delta table path
            retention_hours: Retention period in hours (default: 7 days)
            
        Returns:
            Success status
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, path)
            delta_table.vacuum(retention_hours)
            
            self.logger.info(f"Delta table vacuumed at {path} with retention {retention_hours} hours")
            return True
            
        except Exception as e:
            self.logger.error(f"Vacuum failed for table at {path}: {str(e)}")
            return False
    
    def get_table_history(self, path: str) -> DataFrame:
        """
        Get Delta table version history
        
        Args:
            path: Delta table path
            
        Returns:
            DataFrame with version history
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, path)
            return delta_table.history()
            
        except Exception as e:
            self.logger.error(f"Failed to get history for table at {path}: {str(e)}")
            return self.spark.createDataFrame([], StructType([]))
    
    def _table_exists(self, path: str) -> bool:
        """Check if Delta table exists at path"""
        try:
            DeltaTable.forPath(self.spark, path)
            return True
        except:
            return False
    
    def _table_exists_in_catalog(self, table_name: str) -> bool:
        """Check if table exists in Spark catalog"""
        try:
            self.spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except:
            return False

def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Add standard audit columns to DataFrame
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with audit columns added
    """
    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("ingestion_date", current_date()) \
             .withColumn("processing_batch_id", monotonically_increasing_id())

def add_update_metadata(df: DataFrame) -> DataFrame:
    """
    Add update metadata columns
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with update metadata
    """
    return df.withColumn("last_updated_timestamp", current_timestamp()) \
             .withColumn("last_updated_date", current_date())
