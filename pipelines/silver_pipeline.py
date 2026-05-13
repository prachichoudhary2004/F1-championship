"""
F1 Race Intelligence Lakehouse - Silver Layer Pipeline
Enterprise-grade data transformation and cleansing pipeline
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from services.spark_session import get_spark_session
from services.delta_utils import F1DeltaUtils, add_audit_columns, add_update_metadata
from services.data_quality import F1DataQualityManager, F1_SILVER_CHECKS
from services.logging_config import PipelineLogger, log_data_metrics
from services.monitoring import get_monitoring_service

class F1SilverPipeline:
    """
    Enterprise Silver layer pipeline for F1 Race Intelligence Lakehouse
    Handles data transformation, cleansing, and standardization
    """
    
    def __init__(self):
        self.spark = get_spark_session()
        self.delta_utils = F1DeltaUtils(self.spark)
        self.quality_manager = F1DataQualityManager(self.spark)
        self.monitoring = get_monitoring_service(self.spark)
        self.logger = logging.getLogger(__name__)
    
    def create_silver_races(self) -> bool:
        """Create silver races table with cleansing and enrichment"""
        try:
            with PipelineLogger("Silver Races Processing", self.logger):
                # Read from bronze layer
                races_df = self.spark.read.format("delta").load("data/bronze/races")
                
                # Data cleansing and transformation
                races_df = races_df.filter(col("raceId").isNotNull() & col("year").isNotNull())
                
                # Clean string columns
                races_df = races_df.withColumn("name", trim(col("name"))) \
                                   .withColumn("circuitId", trim(col("circuitId")))
                
                # Standardize timestamps
                races_df = races_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
                                   .withColumn("time", 
                                               when(col("time").isNotNull(), 
                                                   to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))
                                               .otherwise(None))
                
                # Add derived columns
                races_df = races_df.withColumn("race_duration_minutes",
                                              when(col("time").isNotNull(),
                                                   hour(col("time")) * 60 + minute(col("time")) + second(col("time")) / 60.0)
                                              .otherwise(None)) \
                                   .withColumn("race_season_phase",
                                              when(col("round") <= 5, "Early")
                                              .when(col("round") <= 15, "Mid")
                                              .otherwise("Late"))
                
                # Add update metadata
                races_df = add_update_metadata(races_df)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=races_df,
                    path="data/silver/races",
                    table_name="silver.races",
                    partition_by=["year", "round"],
                    z_order_by=["raceId", "year", "round"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("silver.races", races_df)
                    self.logger.info("Silver races table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create silver races table: {str(e)}")
            return False
    
    def create_silver_drivers(self) -> bool:
        """Create silver drivers table with cleansing and enrichment"""
        try:
            with PipelineLogger("Silver Drivers Processing", self.logger):
                # Read from bronze layer
                drivers_df = self.spark.read.format("delta").load("data/bronze/drivers")
                
                # Data cleansing
                drivers_df = drivers_df.filter(col("driverId").isNotNull())
                
                # Clean string columns
                drivers_df = drivers_df.withColumn("forename", trim(col("forename"))) \
                                       .withColumn("surname", trim(col("surname"))) \
                                       .withColumn("nationality", trim(col("nationality"))) \
                                       .withColumn("code", upper(trim(col("code"))))
                
                # Standardize date of birth
                drivers_df = drivers_df.withColumn("dob", to_date(col("dob"), "yyyy-MM-dd"))
                
                # Add derived columns
                drivers_df = drivers_df.withColumn("full_name", concat(col("forename"), lit(" "), col("surname"))) \
                                       .withColumn("driver_age_group",
                                                  when(col("dob") > "2000-01-01", "Gen Z")
                                                  .when(col("dob") > "1980-01-01", "Millennial")
                                                  .otherwise("Gen X/Boomer"))
                
                # Add update metadata
                drivers_df = add_update_metadata(drivers_df)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=drivers_df,
                    path="data/silver/drivers",
                    table_name="silver.drivers",
                    partition_by=["nationality"],
                    z_order_by=["driverId", "surname"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("silver.drivers", drivers_df)
                    self.logger.info("Silver drivers table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create silver drivers table: {str(e)}")
            return False
    
    def create_silver_constructors(self) -> bool:
        """Create silver constructors table with cleansing and enrichment"""
        try:
            with PipelineLogger("Silver Constructors Processing", self.logger):
                # Read from bronze layer
                constructors_df = self.spark.read.format("delta").load("data/bronze/constructors")
                
                # Data cleansing
                constructors_df = constructors_df.filter(col("constructorId").isNotNull())
                
                # Clean string columns
                constructors_df = constructors_df.withColumn("name", trim(col("name"))) \
                                                 .withColumn("nationality", trim(col("nationality")))
                
                # Add update metadata
                constructors_df = add_update_metadata(constructors_df)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=constructors_df,
                    path="data/silver/constructors",
                    table_name="silver.constructors",
                    partition_by=["nationality"],
                    z_order_by=["constructorId", "name"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("silver.constructors", constructors_df)
                    self.logger.info("Silver constructors table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create silver constructors table: {str(e)}")
            return False
    
    def create_silver_circuits(self) -> bool:
        """Create silver circuits table with cleansing and enrichment"""
        try:
            with PipelineLogger("Silver Circuits Processing", self.logger):
                # Read from bronze layer
                circuits_df = self.spark.read.format("delta").load("data/bronze/circuits")
                
                # Data cleansing
                circuits_df = circuits_df.filter(col("circuitId").isNotNull())
                
                # Clean string columns
                circuits_df = circuits_df.withColumn("name", trim(col("name"))) \
                                         .withColumn("location", trim(col("location"))) \
                                         .withColumn("country", trim(col("country")))
                
                # Validate coordinates
                circuits_df = circuits_df.filter(
                    (col("lat").between(-90, 90)) & 
                    (col("lng").between(-180, 180))
                )
                
                # Add derived columns
                circuits_df = circuits_df.withColumn("circuit_region",
                                                    when(col("country").isin("UK", "Germany", "Italy", "France", "Spain", "Belgium"), "Europe")
                                                    .when(col("country").isin("USA", "Canada", "Mexico", "Brazil", "Argentina"), "Americas")
                                                    .when(col("country").isin("Japan", "China", "Singapore", "Malaysia", "Bahrain", "UAE"), "Asia-Pacific")
                                                    .otherwise("Other"))
                
                # Add update metadata
                circuits_df = add_update_metadata(circuits_df)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=circuits_df,
                    path="data/silver/circuits",
                    table_name="silver.circuits",
                    partition_by=["country"],
                    z_order_by=["circuitId", "name"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("silver.circuits", circuits_df)
                    self.logger.info("Silver circuits table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create silver circuits table: {str(e)}")
            return False
    
    def create_silver_performance(self) -> bool:
        """Create silver performance table with joins and business logic"""
        try:
            with PipelineLogger("Silver Performance Processing", self.logger):
                # Read from bronze layer
                results_df = self.spark.read.format("delta").load("data/bronze/results")
                races_df = self.spark.read.format("delta").load("data/bronze/races")
                drivers_df = self.spark.read.format("delta").load("data/bronze/drivers")
                constructors_df = self.spark.read.format("delta").load("data/bronze/constructors")
                
                # Join tables
                performance_df = results_df.join(races_df, "raceId", "inner") \
                                         .join(drivers_df, "driverId", "inner") \
                                         .join(constructors_df, "constructorId", "inner")
                
                # Data cleansing
                performance_df = performance_df.filter(
                    (col("raceId").isNotNull()) &
                    (col("driverId").isNotNull()) &
                    (col("constructorId").isNotNull())
                )
                
                # Clean numeric columns
                performance_df = performance_df.withColumn("position", 
                                                          when(col("position").rlike("^[0-9]+$"), col("position").cast(IntegerType()))
                                                          .otherwise(None)) \
                                               .withColumn("points", col("points").cast(DoubleType())) \
                                               .withColumn("laps", col("laps").cast(IntegerType()))
                
                # Validate ranges
                performance_df = performance_df.filter(
                    (col("position").geq(1) | col("position").isNull()) &
                    (col("position").leq(50) | col("position").isNull()) &
                    (col("points").geq(0) | col("points").isNull()) &
                    (col("points").leq(50) | col("points").isNull()) &
                    (col("laps").geq(1) | col("laps").isNull()) &
                    (col("laps").leq(100) | col("laps").isNull())
                )
                
                # Add derived columns
                performance_df = performance_df.withColumn("finished_race",
                                                           when(col("positionText").isin("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"), True)
                                                           .otherwise(False)) \
                                                              .withColumn("podium_finish",
                                                                          when(col("position").isin(1, 2, 3), True)
                                                                          .otherwise(False)) \
                                                              .withColumn("dnf_status",
                                                                          when(col("positionText").isin("R", "D", "F"), True)
                                                                          .otherwise(False))
                
                # Select relevant columns
                performance_df = performance_df.select(
                    "raceId", "driverId", "constructorId", "position", "positionText", "points", "laps",
                    "finished_race", "podium_finish", "dnf_status", "fastestLap",
                    "year", "round", "name", "circuitId",
                    "forename", "surname", "nationality",
                    "constructorName", "constructorNationality"
                )
                
                # Add update metadata
                performance_df = add_update_metadata(performance_df)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=performance_df,
                    path="data/silver/performance",
                    table_name="silver.performance",
                    partition_by=["year"],
                    z_order_by=["raceId", "driverId", "constructorId"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("silver.performance", performance_df)
                    self.logger.info("Silver performance table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create silver performance table: {str(e)}")
            return False
    
    def create_silver_pit_analysis(self) -> bool:
        """Create silver pit analysis table with joins and business logic"""
        try:
            with PipelineLogger("Silver Pit Analysis Processing", self.logger):
                # Read from bronze layer
                pit_stops_df = self.spark.read.format("delta").load("data/bronze/pit_stops")
                races_df = self.spark.read.format("delta").load("data/bronze/races")
                drivers_df = self.spark.read.format("delta").load("data/bronze/drivers")
                
                # Join tables
                pit_df = pit_stops_df.join(races_df, "raceId", "inner") \
                                     .join(drivers_df, "driverId", "inner")
                
                # Data cleansing
                pit_df = pit_df.filter(
                    (col("raceId").isNotNull()) &
                    (col("driverId").isNotNull())
                )
                
                # Clean numeric columns
                pit_df = pit_df.withColumn("stop", col("stop").cast(IntegerType())) \
                               .withColumn("milliseconds", col("milliseconds").cast(LongType()))
                
                # Validate ranges
                pit_df = pit_df.filter(
                    (col("stop").geq(1)) &
                    (col("stop").leq(10)) &
                    (col("milliseconds").geq(1000)) &
                    (col("milliseconds").leq(300000))  # Max 5 minutes
                )
                
                # Add derived columns
                pit_df = pit_df.withColumn("pit_duration_seconds", col("milliseconds") / 1000.0) \
                               .withColumn("pit_time_category",
                                          when(col("milliseconds") < 20000, "Fast")
                                          .when(col("milliseconds") < 25000, "Normal")
                                          .when(col("milliseconds") < 30000, "Slow")
                                          .otherwise("Very Slow"))
                
                # Select relevant columns
                pit_df = pit_df.select(
                    "raceId", "driverId", "stop", "milliseconds", "pit_duration_seconds", "pit_time_category",
                    "year", "round", "name", "circuitId",
                    "forename", "surname", "nationality"
                )
                
                # Add update metadata
                pit_df = add_update_metadata(pit_df)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=pit_df,
                    path="data/silver/pit_analysis",
                    table_name="silver.pit_analysis",
                    partition_by=["year"],
                    z_order_by=["raceId", "driverId", "stop"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("silver.pit_analysis", pit_df)
                    self.logger.info("Silver pit analysis table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create silver pit analysis table: {str(e)}")
            return False
    
    def run_full_silver_pipeline(self) -> bool:
        """Run complete Silver layer pipeline"""
        try:
            with PipelineLogger("Full Silver Pipeline", self.logger):
                self.monitoring.start_pipeline_monitoring("silver_full_pipeline")
                
                # Process all tables
                tables = [
                    ("races", self.create_silver_races),
                    ("drivers", self.create_silver_drivers),
                    ("constructors", self.create_silver_constructors),
                    ("circuits", self.create_silver_circuits),
                    ("performance", self.create_silver_performance),
                    ("pit_analysis", self.create_silver_pit_analysis)
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
                self.monitoring.end_pipeline_monitoring("silver_full_pipeline", status)
                
                self.logger.info(f"Silver pipeline completed: {success_count}/{len(tables)} tables")
                return success_count > 0
                
        except Exception as e:
            self.monitoring.end_pipeline_monitoring("silver_full_pipeline", "FAILED", str(e))
            self.logger.error(f"Silver pipeline failed: {str(e)}")
            return False

def main():
    """Main function to run Silver pipeline"""
    try:
        pipeline = F1SilverPipeline()
        success = pipeline.run_full_silver_pipeline()
        
        if success:
            print("✓ Silver pipeline completed successfully")
        else:
            print("✗ Silver pipeline failed")
            
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
