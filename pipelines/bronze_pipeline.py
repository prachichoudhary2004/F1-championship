"""
F1 Race Intelligence Lakehouse - Bronze Layer Pipeline
Enterprise-grade Bronze layer processing and management
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from services.spark_session import get_spark_session
from services.delta_utils import F1DeltaUtils
from services.data_quality import F1DataQualityManager
from services.logging_config import PipelineLogger
from services.monitoring import get_monitoring_service

class F1BronzePipeline:
    """
    Enterprise Bronze layer pipeline for F1 Race Intelligence Lakehouse
    Manages raw data processing, validation, and storage
    """
    
    def __init__(self):
        self.spark = get_spark_session()
        self.delta_utils = F1DeltaUtils(self.spark)
        self.quality_manager = F1DataQualityManager(self.spark)
        self.monitoring = get_monitoring_service(self.spark)
        self.logger = logging.getLogger(__name__)
    
    def create_bronze_races(self) -> bool:
        """Create bronze races table with enterprise processing"""
        try:
            with PipelineLogger("Bronze Races Processing", self.logger):
                # Read raw data
                races_df = self.spark.read.option("header", "true").csv("data/raw/races.csv")
                
                # Add audit columns
                races_df = races_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                   .withColumn("ingestion_date", current_date()) \
                                   .withColumn("processing_batch_id", monotonically_increasing_id())
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=races_df,
                    path="data/bronze/races",
                    table_name="bronze.races",
                    partition_by=["year"],
                    z_order_by=["raceId", "year"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("bronze.races", races_df)
                    self.logger.info("Bronze races table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create bronze races table: {str(e)}")
            return False
    
    def create_bronze_drivers(self) -> bool:
        """Create bronze drivers table with enterprise processing"""
        try:
            with PipelineLogger("Bronze Drivers Processing", self.logger):
                # Read raw data
                drivers_df = self.spark.read.json("data/raw/drivers.json")
                
                # Add audit columns
                drivers_df = drivers_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                      .withColumn("ingestion_date", current_date()) \
                                      .withColumn("processing_batch_id", monotonically_increasing_id())
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=drivers_df,
                    path="data/bronze/drivers",
                    table_name="bronze.drivers",
                    z_order_by=["driverId"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("bronze.drivers", drivers_df)
                    self.logger.info("Bronze drivers table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create bronze drivers table: {str(e)}")
            return False
    
    def create_bronze_constructors(self) -> bool:
        """Create bronze constructors table with enterprise processing"""
        try:
            with PipelineLogger("Bronze Constructors Processing", self.logger):
                # Read raw data
                constructors_df = self.spark.read.json("data/raw/constructors.json")
                
                # Add audit columns
                constructors_df = constructors_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                               .withColumn("ingestion_date", current_date()) \
                                               .withColumn("processing_batch_id", monotonically_increasing_id())
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=constructors_df,
                    path="data/bronze/constructors",
                    table_name="bronze.constructors",
                    z_order_by=["constructorId"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("bronze.constructors", constructors_df)
                    self.logger.info("Bronze constructors table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create bronze constructors table: {str(e)}")
            return False
    
    def create_bronze_circuits(self) -> bool:
        """Create bronze circuits table with enterprise processing"""
        try:
            with PipelineLogger("Bronze Circuits Processing", self.logger):
                # Read raw data
                circuits_df = self.spark.read.option("header", "true").csv("data/raw/circuits.csv")
                
                # Add audit columns
                circuits_df = circuits_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                       .withColumn("ingestion_date", current_date()) \
                                       .withColumn("processing_batch_id", monotonically_increasing_id())
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=circuits_df,
                    path="data/bronze/circuits",
                    table_name="bronze.circuits",
                    z_order_by=["circuitId"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("bronze.circuits", circuits_df)
                    self.logger.info("Bronze circuits table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create bronze circuits table: {str(e)}")
            return False
    
    def create_bronze_results(self) -> bool:
        """Create bronze results table with enterprise processing"""
        try:
            with PipelineLogger("Bronze Results Processing", self.logger):
                # Read raw data
                results_df = self.spark.read.json("data/raw/results.json")
                
                # Add audit columns
                results_df = results_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                      .withColumn("ingestion_date", current_date()) \
                                      .withColumn("processing_batch_id", monotonically_increasing_id())
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=results_df,
                    path="data/bronze/results",
                    table_name="bronze.results",
                    partition_by=["raceId"],
                    z_order_by=["resultId", "raceId"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("bronze.results", results_df)
                    self.logger.info("Bronze results table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create bronze results table: {str(e)}")
            return False
    
    def create_bronze_lap_times(self) -> bool:
        """Create bronze lap times table with enterprise processing"""
        try:
            with PipelineLogger("Bronze Lap Times Processing", self.logger):
                # Read raw data (multiple files)
                lap_times_df = self.spark.read.option("header", "true").csv("data/raw/lap_times")
                
                # Add audit columns
                lap_times_df = lap_times_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                          .withColumn("ingestion_date", current_date()) \
                                          .withColumn("processing_batch_id", monotonically_increasing_id())
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=lap_times_df,
                    path="data/bronze/lap_times",
                    table_name="bronze.lap_times",
                    partition_by=["raceId"],
                    z_order_by=["raceId", "driverId", "lap"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("bronze.lap_times", lap_times_df)
                    self.logger.info("Bronze lap times table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create bronze lap times table: {str(e)}")
            return False
    
    def create_bronze_pit_stops(self) -> bool:
        """Create bronze pit stops table with enterprise processing"""
        try:
            with PipelineLogger("Bronze Pit Stops Processing", self.logger):
                # Read raw data
                pit_stops_df = self.spark.read.json("data/raw/pit_stops.json")
                
                # Add audit columns
                pit_stops_df = pit_stops_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                           .withColumn("ingestion_date", current_date()) \
                                           .withColumn("processing_batch_id", monotonically_increasing_id())
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=pit_stops_df,
                    path="data/bronze/pit_stops",
                    table_name="bronze.pit_stops",
                    partition_by=["raceId"],
                    z_order_by=["raceId", "driverId", "stop"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("bronze.pit_stops", pit_stops_df)
                    self.logger.info("Bronze pit stops table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create bronze pit stops table: {str(e)}")
            return False
    
    def create_bronze_qualifying(self) -> bool:
        """Create bronze qualifying table with enterprise processing"""
        try:
            with PipelineLogger("Bronze Qualifying Processing", self.logger):
                # Read raw data (multiple files)
                qualifying_df = self.spark.read.json("data/raw/qualifying")
                
                # Add audit columns
                qualifying_df = qualifying_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                            .withColumn("ingestion_date", current_date()) \
                                            .withColumn("processing_batch_id", monotonically_increasing_id())
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=qualifying_df,
                    path="data/bronze/qualifying",
                    table_name="bronze.qualifying",
                    partition_by=["raceId"],
                    z_order_by=["raceId", "driverId"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("bronze.qualifying", qualifying_df)
                    self.logger.info("Bronze qualifying table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create bronze qualifying table: {str(e)}")
            return False
    
    def run_full_bronze_pipeline(self) -> bool:
        """Run the complete Bronze layer pipeline"""
        try:
            with PipelineLogger("Full Bronze Pipeline", self.logger):
                self.monitoring.start_pipeline_monitoring("bronze_full_pipeline")
                
                # Process all tables
                tables = [
                    ("races", self.create_bronze_races),
                    ("drivers", self.create_bronze_drivers),
                    ("constructors", self.create_bronze_constructors),
                    ("circuits", self.create_bronze_circuits),
                    ("results", self.create_bronze_results),
                    ("lap_times", self.create_bronze_lap_times),
                    ("pit_stops", self.create_bronze_pit_stops),
                    ("qualifying", self.create_bronze_qualifying)
                ]
                
                success_count = 0
                for table_name, create_func in tables:
                    try:
                        if create_func():
                            success_count += 1
                            self.logger.info(f"✓ {table_name} table created")
                        else:
                            self.logger.error(f"✗ {table_name} table failed")
                    except Exception as e:
                        self.logger.error(f"✗ {table_name} table error: {str(e)}")
                
                status = "SUCCESS" if success_count == len(tables) else "PARTIAL"
                self.monitoring.end_pipeline_monitoring("bronze_full_pipeline", status)
                
                self.logger.info(f"Bronze pipeline completed: {success_count}/{len(tables)} tables")
                return success_count > 0
                
        except Exception as e:
            self.monitoring.end_pipeline_monitoring("bronze_full_pipeline", "FAILED", str(e))
            self.logger.error(f"Bronze pipeline failed: {str(e)}")
            return False

def main():
    """Main function to run Bronze pipeline"""
    try:
        pipeline = F1BronzePipeline()
        success = pipeline.run_full_bronze_pipeline()
        
        if success:
            print("✓ Bronze pipeline completed successfully")
        else:
            print("✗ Bronze pipeline failed")
            
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
