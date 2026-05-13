"""
F1 Race Intelligence Lakehouse - Gold Layer Pipeline
Enterprise-grade business intelligence and analytics pipeline
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from services.spark_session import get_spark_session
from services.delta_utils import F1DeltaUtils, add_update_metadata
from services.data_quality import F1DataQualityManager, F1_GOLD_CHECKS
from services.logging_config import PipelineLogger, log_data_metrics
from services.monitoring import get_monitoring_service

class F1GoldPipeline:
    """
    Enterprise Gold layer pipeline for F1 Race Intelligence Lakehouse
    Handles business intelligence, analytics, and ML-ready datasets
    """
    
    def __init__(self):
        self.spark = get_spark_session()
        self.delta_utils = F1DeltaUtils(self.spark)
        self.quality_manager = F1DataQualityManager(self.spark)
        self.monitoring = get_monitoring_service(self.spark)
        self.logger = logging.getLogger(__name__)
    
    def create_gold_driver_statistics(self) -> bool:
        """Create gold driver statistics with comprehensive analytics"""
        try:
            with PipelineLogger("Gold Driver Statistics Processing", self.logger):
                # Read from silver layer
                performance_df = self.spark.read.format("delta").load("data/silver/performance")
                
                # Driver performance aggregations
                driver_window = Window.partitionBy("driverId", "year")
                
                driver_stats = performance_df.groupBy("driverId", "forename", "surname", "nationality", "constructorId", "constructorName", "year") \
                    .agg(
                        count("raceId").alias("total_races"),
                        sum(when(col("position") == 1, 1).otherwise(0)).alias("total_wins"),
                        sum(when(col("podium_finish") == True, 1).otherwise(0)).alias("total_podiums"),
                        sum("points").alias("total_points"),
                        avg(when(col("finished_race") == True, col("position")).otherwise(None)).alias("average_finish_position"),
                        sum(when(col("dnf_status") == True, 1).otherwise(0)).alias("dnf_count"),
                        sum(when(col("fastestLap") == 1, 1).otherwise(0)).alias("fastest_laps"),
                        min("position").alias("career_best_position"),
                        max("position").alias("career_worst_position"),
                        stddev(when(col("finished_race") == True, col("position"))).alias("position_stddev")
                    )
                
                # Add derived metrics
                driver_stats = driver_stats.withColumn("win_rate", col("total_wins") / col("total_races")) \
                                          .withColumn("podium_rate", col("total_podiums") / col("total_races")) \
                                          .withColumn("dnf_rate", col("dnf_count") / col("total_races")) \
                                          .withColumn("points_per_race", col("total_points") / col("total_races")) \
                                          .withColumn("finish_rate", (col("total_races") - col("dnf_count")) / col("total_races")) \
                                          .withColumn("consistency_score", 
                                                     when(col("position_stddev").isNotNull(),
                                                          1 - (col("position_stddev") / col("average_finish_position")))
                                                     .otherwise(0.0))
                
                # Add update metadata
                driver_stats = add_update_metadata(driver_stats)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=driver_stats,
                    path="data/gold/driver_statistics",
                    table_name="gold.driver_statistics",
                    partition_by=["year"],
                    z_order_by=["driverId", "year"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("gold.driver_statistics", driver_stats)
                    self.logger.info("Gold driver statistics table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create gold driver statistics: {str(e)}")
            return False
    
    def create_gold_constructor_rankings(self) -> bool:
        """Create gold constructor rankings with team analytics"""
        try:
            with PipelineLogger("Gold Constructor Rankings Processing", self.logger):
                # Read from silver layer
                performance_df = self.spark.read.format("delta").load("data/silver/performance")
                
                # Constructor performance aggregations
                constructor_stats = performance_df.groupBy("constructorId", "constructorName", "constructorNationality", "year") \
                    .agg(
                        count("raceId").alias("total_races"),
                        sum(when(col("position") == 1, 1).otherwise(0)).alias("total_wins"),
                        sum(when(col("podium_finish") == True, 1).otherwise(0)).alias("total_podiums"),
                        sum("points").alias("total_points"),
                        countDistinct("driverId").alias("driver_count"),
                        avg(when(col("finished_race") == True, col("position")).otherwise(None)).alias("average_team_position")
                    )
                
                # Add derived metrics
                constructor_stats = constructor_stats.withColumn("team_win_rate", col("total_wins") / col("total_races")) \
                                                   .withColumn("team_podium_rate", col("total_podiums") / col("total_races")) \
                                                   .withColumn("points_per_race", col("total_points") / col("total_races")) \
                                                   .withColumn("dominance_score", 
                                                              (col("total_wins") * 25 + col("total_podiums") * 18 + col("total_points")) / col("total_races"))
                
                # Add update metadata
                constructor_stats = add_update_metadata(constructor_stats)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=constructor_stats,
                    path="data/gold/constructor_rankings",
                    table_name="gold.constructor_rankings",
                    partition_by=["year"],
                    z_order_by=["constructorId", "year"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("gold.constructor_rankings", constructor_stats)
                    self.logger.info("Gold constructor rankings table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create gold constructor rankings: {str(e)}")
            return False
    
    def create_gold_lap_time_trends(self) -> bool:
        """Create gold lap time trends with pace analytics"""
        try:
            with PipelineLogger("Gold Lap Time Trends Processing", self.logger):
                # Read from bronze layer
                lap_times_df = self.spark.read.format("delta").load("data/bronze/lap_times")
                
                # Lap time aggregations
                lap_trends = lap_times_df.groupBy("raceId", "year", "driverId", "constructorId") \
                    .agg(
                        min("milliseconds").alias("fastest_lap_time"),
                        max("milliseconds").alias("slowest_lap_time"),
                        avg("milliseconds").alias("average_lap_time"),
                        count("lap").alias("total_laps"),
                        stddev("milliseconds").alias("lap_time_stddev")
                    )
                
                # Add derived metrics
                lap_trends = lap_trends.withColumn("pace_consistency", 
                                                  1 - (col("lap_time_stddev") / col("average_lap_time"))) \
                                         .withColumn("lap_degradation_rate", 
                                                    (col("slowest_lap_time") - col("fastest_lap_time")) / col("total_laps")) \
                                         .withColumn("pace_improvement_potential", 
                                                    (col("average_lap_time") - col("fastest_lap_time")) / col("average_lap_time"))
                
                # Add update metadata
                lap_trends = add_update_metadata(lap_trends)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=lap_trends,
                    path="data/gold/lap_time_trends",
                    table_name="gold.lap_time_trends",
                    partition_by=["year"],
                    z_order_by=["raceId", "driverId", "lap"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("gold.lap_time_trends", lap_trends)
                    self.logger.info("Gold lap time trends table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create gold lap time trends: {str(e)}")
            return False
    
    def create_gold_pit_stop_efficiency(self) -> bool:
        """Create gold pit stop efficiency with team performance metrics"""
        try:
            with PipelineLogger("Gold Pit Stop Efficiency Processing", self.logger):
                # Read from silver layer
                pit_df = self.spark.read.format("delta").load("data/silver/pit_analysis")
                
                # Pit stop aggregations
                pit_efficiency = pit_df.groupBy("raceId", "year", "driverId", "constructorId") \
                    .agg(
                        count("stop").alias("total_pit_stops"),
                        min("milliseconds").alias("fastest_pit_time"),
                        max("milliseconds").alias("slowest_pit_time"),
                        avg("milliseconds").alias("average_pit_time"),
                        sum("milliseconds").alias("total_pit_time")
                    )
                
                # Add derived metrics
                pit_efficiency = pit_efficiency.withColumn("pit_efficiency_score", 10000 / col("average_pit_time")) \
                                               .withColumn("pit_consistency", 
                                                          1 - ((col("slowest_pit_time") - col("fastest_pit_time")) / col("average_pit_time"))) \
                                               .withColumn("time_lost_in_pits", col("total_pit_time") / 1000.0)
                
                # Add update metadata
                pit_efficiency = add_update_metadata(pit_efficiency)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=pit_efficiency,
                    path="data/gold/pit_stop_efficiency",
                    table_name="gold.pit_stop_efficiency",
                    partition_by=["year"],
                    z_order_by=["constructorId", "driverId", "year"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("gold.pit_stop_efficiency", pit_efficiency)
                    self.logger.info("Gold pit stop efficiency table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create gold pit stop efficiency: {str(e)}")
            return False
    
    def create_gold_race_pace_analysis(self) -> bool:
        """Create gold race pace analysis with comprehensive race metrics"""
        try:
            with PipelineLogger("Gold Race Pace Analysis Processing", self.logger):
                # Read from silver and bronze layers
                performance_df = self.spark.read.format("delta").load("data/silver/performance")
                lap_times_df = self.spark.read.format("delta").load("data/bronze/lap_times")
                
                # Join performance with lap times
                race_pace = performance_df.join(lap_times_df, 
                                               (performance_df.raceId == lap_times_df.raceId) & 
                                               (performance_df.driverId == lap_times_df.driverId),
                                               "left")
                
                # Race pace aggregations
                race_pace_analysis = race_pace.groupBy("raceId", "year", "driverId", "constructorId", "position") \
                    .agg(
                        avg("lap_times.milliseconds").alias("average_race_lap_time"),
                        min("lap_times.milliseconds").alias("fastest_race_lap"),
                        max("lap_times.lap").alias("total_race_laps")
                    )
                
                # Add derived metrics
                race_pace_analysis = race_pace_analysis.withColumn("race_finish_time", 
                                                                  col("average_race_lap_time") * col("total_race_laps"))
                
                # Add update metadata
                race_pace_analysis = add_update_metadata(race_pace_analysis)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=race_pace_analysis,
                    path="data/gold/race_pace_analysis",
                    table_name="gold.race_pace_analysis",
                    partition_by=["year"],
                    z_order_by=["raceId", "driverId"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("gold.race_pace_analysis", race_pace_analysis)
                    self.logger.info("Gold race pace analysis table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create gold race pace analysis: {str(e)}")
            return False
    
    def create_gold_championship_standings(self) -> bool:
        """Create gold championship standings with rankings and analysis"""
        try:
            with PipelineLogger("Gold Championship Standings Processing", self.logger):
                # Read from gold driver statistics
                driver_stats_df = self.spark.read.format("delta").load("data/gold/driver_statistics")
                
                # Calculate championship position by year
                year_window = Window.partitionBy("year").orderBy(desc("total_points"), desc("total_wins"), desc("total_podiums"))
                
                championship_standings = driver_stats_df.withColumn("championship_position", 
                                                                   row_number().over(year_window))
                
                # Add points gap to leader
                leader_window = Window.partitionBy("year")
                championship_standings = championship_standings.withColumn("leader_points", 
                                                                         first("total_points").over(leader_window.orderBy(desc("total_points"))))
                
                championship_standings = championship_standings.withColumn("points_gap_to_leader", 
                                                                         col("total_points") - col("leader_points"))
                
                # Add championship battle status
                championship_standings = championship_standings.withColumn("championship_battle_status",
                                                                         when(col("points_gap_to_leader") <= 25, "Contender")
                                                                         .when(col("points_gap_to_leader") <= 50, "Dark Horse")
                                                                         .otherwise("Out of Contention"))
                
                # Select final columns
                championship_standings = championship_standings.select(
                    "driverId", "forename", "surname", "nationality", "year", "championship_position",
                    "total_points", "total_wins", "total_podiums", "total_races",
                    "points_gap_to_leader", "championship_battle_status",
                    "win_rate", "podium_rate", "consistency_score"
                )
                
                # Add update metadata
                championship_standings = add_update_metadata(championship_standings)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=championship_standings,
                    path="data/gold/championship_standings",
                    table_name="gold.championship_standings",
                    partition_by=["year"],
                    z_order_by=["year", "championship_position"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("gold.championship_standings", championship_standings)
                    self.logger.info("Gold championship standings table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create gold championship standings: {str(e)}")
            return False
    
    def create_gold_qualifying_vs_race(self) -> bool:
        """Create gold qualifying vs race performance analysis"""
        try:
            with PipelineLogger("Gold Qualifying vs Race Processing", self.logger):
                # Read from bronze and silver layers
                qualifying_df = self.spark.read.format("delta").load("data/bronze/qualifying")
                performance_df = self.spark.read.format("delta").load("data/silver/performance")
                
                # Join qualifying with race results
                qual_vs_race = qualifying_df.join(performance_df,
                                                 (qualifying_df.raceId == performance_df.raceId) &
                                                 (qualifying_df.driverId == performance_df.driverId),
                                                 "inner")
                
                # Calculate position changes
                qual_vs_race = qual_vs_race.withColumn("qualifying_position", col("position")) \
                                          .withColumn("race_position", col("performance.position")) \
                                          .withColumn("position_change", col("qualifying_position") - col("race_position")) \
                                          .withColumn("overtakes_made",
                                                     when(col("qualifying_position") > col("race_position"),
                                                          col("qualifying_position") - col("race_position"))
                                                     .otherwise(0)) \
                                          .withColumn("positions_lost",
                                                     when(col("qualifying_position") < col("race_position"),
                                                          col("race_position") - col("qualifying_position"))
                                                     .otherwise(0))
                
                # Add qualifying performance score
                qual_vs_race = qual_vs_race.withColumn("qualifying_performance_score",
                                                       when(col("qualifying_position") <= 3, 10)
                                                       .when(col("qualifying_position") <= 10, 5)
                                                       .otherwise(1))
                
                # Select relevant columns
                qual_vs_race = qual_vs_race.select(
                    "raceId", "year", "driverId", "constructorId",
                    "qualifying_position", "race_position", "position_change",
                    "overtakes_made", "positions_lost", "qualifying_performance_score",
                    "forename", "surname", "nationality",
                    "constructorName", "constructorNationality"
                )
                
                # Add update metadata
                qual_vs_race = add_update_metadata(qual_vs_race)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=qual_vs_race,
                    path="data/gold/qualifying_vs_race",
                    table_name="gold.qualifying_vs_race",
                    partition_by=["year"],
                    z_order_by=["raceId", "driverId"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("gold.qualifying_vs_race", qual_vs_race)
                    self.logger.info("Gold qualifying vs race table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create gold qualifying vs race: {str(e)}")
            return False
    
    def create_gold_driver_consistency(self) -> bool:
        """Create gold driver consistency with rolling calculations"""
        try:
            with PipelineLogger("Gold Driver Consistency Processing", self.logger):
                # Read from gold driver statistics
                driver_stats_df = self.spark.read.format("delta").load("data/gold/driver_statistics")
                
                # Define window for rolling calculations (last 5 races by year)
                driver_window = Window.partitionBy("driverId").orderBy("year").rowsBetween(-4, 0)
                
                # Calculate rolling metrics
                driver_consistency = driver_stats_df.withColumn("rolling_average_position",
                                                               avg("average_finish_position").over(driver_window)) \
                                                  .withColumn("rolling_points",
                                                             sum("total_points").over(driver_window)) \
                                                  .withColumn("position_variance",
                                                             variance("average_finish_position").over(driver_window))
                
                # Add consistency rating
                driver_consistency = driver_consistency.withColumn("consistency_rating",
                                                                 when(col("position_variance") < 2, "Very Consistent")
                                                                 .when(col("position_variance") < 5, "Consistent")
                                                                 .when(col("position_variance") < 10, "Inconsistent")
                                                                 .otherwise("Very Inconsistent"))
                
                # Add form trend
                driver_consistency = driver_consistency.withColumn("form_trend",
                                                                 when(col("rolling_average_position") < 10, "Good Form")
                                                                 .when(col("rolling_average_position") < 15, "Average Form")
                                                                 .otherwise("Poor Form"))
                
                # Add update metadata
                driver_consistency = add_update_metadata(driver_consistency)
                
                # Create Delta table
                success = self.delta_utils.create_delta_table(
                    df=driver_consistency,
                    path="data/gold/driver_consistency",
                    table_name="gold.driver_consistency",
                    partition_by=["year"],
                    z_order_by=["driverId", "year"]
                )
                
                if success:
                    self.monitoring.record_table_metrics("gold.driver_consistency", driver_consistency)
                    self.logger.info("Gold driver consistency table created successfully")
                
                return success
                
        except Exception as e:
            self.logger.error(f"Failed to create gold driver consistency: {str(e)}")
            return False
    
    def run_full_gold_pipeline(self) -> bool:
        """Run complete Gold layer pipeline"""
        try:
            with PipelineLogger("Full Gold Pipeline", self.logger):
                self.monitoring.start_pipeline_monitoring("gold_full_pipeline")
                
                # Process all tables
                tables = [
                    ("driver_statistics", self.create_gold_driver_statistics),
                    ("constructor_rankings", self.create_gold_constructor_rankings),
                    ("lap_time_trends", self.create_gold_lap_time_trends),
                    ("pit_stop_efficiency", self.create_gold_pit_stop_efficiency),
                    ("race_pace_analysis", self.create_gold_race_pace_analysis),
                    ("championship_standings", self.create_gold_championship_standings),
                    ("qualifying_vs_race", self.create_gold_qualifying_vs_race),
                    ("driver_consistency", self.create_gold_driver_consistency)
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
                self.monitoring.end_pipeline_monitoring("gold_full_pipeline", status)
                
                self.logger.info(f"Gold pipeline completed: {success_count}/{len(tables)} tables")
                return success_count > 0
                
        except Exception as e:
            self.monitoring.end_pipeline_monitoring("gold_full_pipeline", "FAILED", str(e))
            self.logger.error(f"Gold pipeline failed: {str(e)}")
            return False

def main():
    """Main function to run Gold pipeline"""
    try:
        pipeline = F1GoldPipeline()
        success = pipeline.run_full_gold_pipeline()
        
        if success:
            print("✓ Gold pipeline completed successfully")
        else:
            print("✗ Gold pipeline failed")
            
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
