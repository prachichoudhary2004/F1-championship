"""
F1 Race Intelligence Lakehouse - Bronze Layer Ingestion Pipeline
Enterprise-grade data ingestion for raw F1 data processing
"""

import os
import yaml
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from services.spark_session import get_spark_session
from services.delta_utils import F1DeltaUtils, add_audit_columns
from services.data_quality import F1DataQualityManager, F1_BRONZE_CHECKS
from services.logging_config import PipelineLogger, log_data_metrics
from services.monitoring import get_monitoring_service

class F1BronzeIngestionPipeline:
    """
    Enterprise-grade Bronze layer ingestion pipeline for F1 Race Intelligence Lakehouse
    Handles raw data ingestion with comprehensive quality checks and monitoring
    """
    
    def __init__(self, config_path: str = "configs/bronze_config.yaml"):
        self.config = self._load_config(config_path)
        self.spark = get_spark_session()
        self.delta_utils = F1DeltaUtils(self.spark)
        self.quality_manager = F1DataQualityManager(self.spark)
        self.monitoring = get_monitoring_service(self.spark)
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            self.logger.error(f"Failed to load config from {config_path}: {str(e)}")
            raise
    
    def run_ingestion(self, table_name: Optional[str] = None) -> bool:
        """
        Run the complete ingestion pipeline
        
        Args:
            table_name: Optional specific table to ingest. If None, ingests all tables.
            
        Returns:
            Success status
        """
        with PipelineLogger("Bronze Ingestion Pipeline", self.logger) as logger:
            try:
                # Start monitoring
                execution_id = self.monitoring.start_pipeline_monitoring("bronze_ingestion")
                
                # Get tables to process
                tables_to_process = self._get_tables_to_process(table_name)
                
                if not tables_to_process:
                    logger.error("No tables found to process")
                    return False
                
                logger.info(f"Processing {len(tables_to_process)} tables: {', '.join(tables_to_process)}")
                
                # Process each table
                success_count = 0
                for table_config in tables_to_process:
                    try:
                        if self._ingest_table(table_config):
                            success_count += 1
                            logger.info(f"Successfully ingested table: {table_config['target_table']}")
                        else:
                            logger.error(f"Failed to ingest table: {table_config['target_table']}")
                    except Exception as e:
                        logger.error(f"Error processing table {table_config['target_table']}: {str(e)}")
                
                # End monitoring
                status = "SUCCESS" if success_count == len(tables_to_process) else "PARTIAL"
                self.monitoring.end_pipeline_monitoring("bronze_ingestion", status)
                
                logger.info(f"Ingestion completed: {success_count}/{len(tables_to_process)} tables processed successfully")
                return success_count > 0
                
            except Exception as e:
                self.monitoring.end_pipeline_monitoring("bronze_ingestion", "FAILED", str(e))
                logger.error(f"Bronze ingestion pipeline failed: {str(e)}")
                return False
    
    def _get_tables_to_process(self, table_name: Optional[str]) -> List[Dict[str, Any]]:
        """Get list of tables to process based on configuration"""
        tables_config = self.config.get('bronze', {}).get('tables', {})
        
        if table_name:
            if table_name in tables_config:
                return [tables_config[table_name]]
            else:
                self.logger.error(f"Table '{table_name}' not found in configuration")
                return []
        
        return list(tables_config.values())
    
    def _ingest_table(self, table_config: Dict[str, Any]) -> bool:
        """
        Ingest a single table
        
        Args:
            table_config: Table configuration dictionary
            
        Returns:
            Success status
        """
        table_name = table_config['target_table']
        source_file = table_config['source_file']
        
        try:
            self.logger.info(f"Starting ingestion for table: {table_name}")
            
            # Read source data
            df = self._read_source_data(source_file)
            
            if df is None or df.count() == 0:
                self.logger.warning(f"No data found for table: {table_name}")
                return False
            
            # Add audit columns
            df = add_audit_columns(df)
            
            # Add source metadata
            df = self._add_source_metadata(df, source_file)
            
            # Run data quality checks
            if not self._run_quality_checks(df, table_name):
                self.logger.error(f"Data quality checks failed for table: {table_name}")
                return False
            
            # Write to Delta table
            if not self._write_to_delta(df, table_config):
                self.logger.error(f"Failed to write Delta table: {table_name}")
                return False
            
            # Record metrics
            self.monitoring.record_table_metrics(table_name, df)
            
            # Log metrics
            log_data_metrics(table_name, df.count(), df.columns, self.logger)
            
            self.logger.info(f"Successfully ingested table: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to ingest table {table_name}: {str(e)}")
            return False
    
    def _read_source_data(self, source_file: str) -> Optional[DataFrame]:
        """
        Read source data from file
        
        Args:
            source_file: Source file path
            
        Returns:
            DataFrame or None if failed
        """
        try:
            source_path = os.path.join(
                self.config['bronze']['source']['raw_data_path'],
                source_file
            )
            
            if not os.path.exists(source_path):
                self.logger.error(f"Source file not found: {source_path}")
                return None
            
            # Determine file format and read accordingly
            if source_file.endswith('.csv'):
                df = self.spark.read.option("header", "true").csv(source_path)
            elif source_file.endswith('.json'):
                df = self.spark.read.json(source_path)
            elif os.path.isdir(source_path):
                # Handle directory with multiple files
                if source_file.endswith('.csv'):
                    df = self.spark.read.option("header", "true").csv(source_path)
                else:
                    df = self.spark.read.json(source_path)
            else:
                self.logger.error(f"Unsupported file format: {source_file}")
                return None
            
            self.logger.info(f"Read {df.count()} rows from {source_path}")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read source data from {source_file}: {str(e)}")
            return None
    
    def _add_source_metadata(self, df: DataFrame, source_file: str) -> DataFrame:
        """
        Add source metadata columns to DataFrame
        
        Args:
            df: Input DataFrame
            source_file: Source file name
            
        Returns:
            DataFrame with source metadata
        """
        try:
            source_path = os.path.join(
                self.config['bronze']['source']['raw_data_path'],
                source_file
            )
            
            # Get file metadata
            file_size = os.path.getsize(source_path) if os.path.exists(source_path) else 0
            file_modified = datetime.fromtimestamp(os.path.getmtime(source_path)) if os.path.exists(source_path) else None
            
            return df.withColumn("source_file_name", lit(source_file)) \
                     .withColumn("source_file_size", lit(file_size)) \
                     .withColumn("source_file_modified", lit(file_modified))
                     
        except Exception as e:
            self.logger.warning(f"Failed to add source metadata: {str(e)}")
            return df
    
    def _run_quality_checks(self, df: DataFrame, table_name: str) -> bool:
        """
        Run data quality checks on DataFrame
        
        Args:
            df: DataFrame to check
            table_name: Table name for logging
            
        Returns:
            True if all critical checks pass
        """
        try:
            # Run predefined quality checks
            results = self.quality_manager.run_quality_checks(df, table_name, F1_BRONZE_CHECKS)
            
            # Check for failed critical checks
            failed_checks = [r for r in results if r.status == 'FAIL']
            
            if failed_checks:
                self.logger.error(f"Critical quality checks failed for {table_name}:")
                for check in failed_checks:
                    self.logger.error(f"  - {check.check_name}: {check.error_message}")
                return False
            
            # Log warnings
            warning_checks = [r for r in results if r.status == 'WARNING']
            if warning_checks:
                self.logger.warning(f"Quality warnings for {table_name}:")
                for check in warning_checks:
                    self.logger.warning(f"  - {check.check_name}: {check.error_message}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to run quality checks for {table_name}: {str(e)}")
            return False
    
    def _write_to_delta(self, df: DataFrame, table_config: Dict[str, Any]) -> bool:
        """
        Write DataFrame to Delta table
        
        Args:
            df: DataFrame to write
            table_config: Table configuration
            
        Returns:
            Success status
        """
        try:
            target_table = table_config['target_table']
            target_path = os.path.join(
                self.config['bronze']['base_path'],
                target_table.replace('.', '/')
            )
            
            partition_by = table_config.get('partition_by', [])
            z_order_by = table_config.get('z_order_by', [])
            
            # Create Delta table
            success = self.delta_utils.create_delta_table(
                df=df,
                path=target_path,
                table_name=target_table,
                partition_by=partition_by,
                z_order_by=z_order_by
            )
            
            if success:
                self.logger.info(f"Successfully created Delta table: {target_table}")
            else:
                self.logger.error(f"Failed to create Delta table: {target_table}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to write to Delta table: {str(e)}")
            return False
    
    def get_ingestion_status(self) -> Dict[str, Any]:
        """Get current ingestion pipeline status"""
        try:
            pipeline_summary = self.monitoring.get_pipeline_summary("bronze_ingestion")
            table_summary = self.monitoring.get_table_summary()
            health_status = self.monitoring.check_pipeline_health()
            
            return {
                "pipeline_status": pipeline_summary,
                "table_status": table_summary,
                "health_status": health_status,
                "last_updated": str(datetime.now())
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get ingestion status: {str(e)}")
            return {"error": str(e)}
    
    def validate_source_data(self) -> Dict[str, Any]:
        """
        Validate all source data files
        
        Returns:
            Validation results
        """
        try:
            validation_results = {}
            tables_config = self.config.get('bronze', {}).get('tables', {})
            
            for table_name, table_config in tables_config.items():
                source_file = table_config['source_file']
                source_path = os.path.join(
                    self.config['bronze']['source']['raw_data_path'],
                    source_file
                )
                
                result = {
                    "file_exists": os.path.exists(source_path),
                    "file_size": os.path.getsize(source_path) if os.path.exists(source_path) else 0,
                    "last_modified": datetime.fromtimestamp(os.path.getmtime(source_path)).isoformat() if os.path.exists(source_path) else None,
                    "readable": False
                }
                
                if result["file_exists"]:
                    try:
                        # Try to read a sample
                        if source_file.endswith('.csv'):
                            sample_df = self.spark.read.option("header", "true").option("maxRowsPerFile", 10).csv(source_path)
                        else:
                            sample_df = self.spark.read.json(source_path)
                        
                        result["readable"] = True
                        result["sample_columns"] = sample_df.columns
                        result["sample_row_count"] = sample_df.count()
                        
                    except Exception as e:
                        result["read_error"] = str(e)
                
                validation_results[table_name] = result
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Failed to validate source data: {str(e)}")
            return {"error": str(e)}

def main():
    """Main function to run the Bronze ingestion pipeline"""
    try:
        # Initialize pipeline
        pipeline = F1BronzeIngestionPipeline()
        
        # Validate source data
        print("Validating source data...")
        validation_results = pipeline.validate_source_data()
        
        for table, result in validation_results.items():
            status = "✓" if result.get("readable", False) else "✗"
            print(f"{status} {table}: {result.get('file_size', 0)} bytes")
        
        # Run ingestion
        print("\nStarting Bronze layer ingestion...")
        success = pipeline.run_ingestion()
        
        if success:
            print("✓ Bronze ingestion completed successfully")
        else:
            print("✗ Bronze ingestion failed")
        
        # Get status
        status = pipeline.get_ingestion_status()
        print(f"\nPipeline Status: {status.get('health_status', {}).get('status', 'UNKNOWN')}")
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
