"""
F1 Race Intelligence Lakehouse - Lap Time Forecasting Model
Enterprise-grade time series forecasting for F1 lap time prediction
"""

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import logging
from typing import Dict, List, Any, Tuple
import joblib
import os

from services.spark_session import get_spark_session
from services.logging_config import setup_logging

class F1LapTimeForecasting:
    """
    Enterprise-grade lap time forecasting model for F1 race prediction
    Uses advanced time series and regression techniques
    """
    
    def __init__(self):
        self.spark = get_spark_session()
        self.logger = setup_logging("F1-Lap-Time-Forecasting")
        self.model = None
        self.feature_columns = []
        self.target_column = "predicted_lap_time"
        
    def load_and_prepare_data(self) -> DataFrame:
        """
        Load and prepare data for lap time forecasting
        
        Returns:
            Prepared Spark DataFrame
        """
        try:
            self.logger.info("Loading data for lap time forecasting...")
            
            # Load gold and silver layer tables
            lap_trends = self.spark.read.format("delta").load("data/gold/lap_time_trends")
            performance = self.spark.read.format("delta").load("data/silver/performance")
            races = self.spark.read.format("delta").load("data/silver/races")
            circuits = self.spark.read.format("delta").load("data/silver/circuits")
            
            # Join data
            data = lap_trends.join(performance,
                                 (lap_trends.raceId == performance.raceId) &
                                 (lap_trends.driverId == performance.driverId),
                                 "left") \
                            .join(races, lap_trends.raceId == races.raceId, "left") \
                            .join(circuits, races.circuitId == circuits.circuitId, "left")
            
            # Filter for recent years and complete data
            data = data.filter(col("year") >= 2015) \
                       .filter(col("average_lap_time").isNotNull()) \
                       .filter(col("total_laps") >= 10)  # Minimum laps for meaningful analysis
            
            # Remove outliers (extremely slow/fast laps)
            data = data.filter(col("average_lap_time").between(60000, 180000))  # 1-3 minutes
            
            self.logger.info(f"Loaded {data.count()} records for forecasting")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load and prepare data: {str(e)}")
            raise
    
    def feature_engineering(self, data: DataFrame) -> DataFrame:
        """
        Perform advanced feature engineering for lap time forecasting
        
        Args:
            data: Input DataFrame
            
        Returns:
            DataFrame with engineered features
        """
        try:
            self.logger.info("Performing feature engineering for lap time forecasting...")
            
            # Time-based features
            data = data.withColumn("race_progress", 
                                  col("lap") / col("total_laps")) \
                         .withColumn("is_early_lap", 
                                  when(col("lap") <= 5, 1).otherwise(0)) \
                         .withColumn("is_late_lap", 
                                  when(col("lap") >= col("total_laps") - 5, 1).otherwise(0))
            
            # Driver performance features
            data = data.withColumn("driver_experience", 
                                  year(current_date()) - year(col("dob"))) \
                         .withColumn("driver_form", 
                                  when(col("position") <= 5, 1.0)
                                  .when(col("position") <= 10, 0.5)
                                  .otherwise(0.0))
            
            # Circuit characteristics
            data = data.withColumn("circuit_type", 
                                  when(col("circuit_region") == "Europe", "European")
                                  .when(col("circuit_region") == "Americas", "American")
                                  .when(col("circuit_region") == "Asia-Pacific", "Asian")
                                  .otherwise("Other")) \
                         .withColumn("is_street_circuit", 
                                  when(col("name").rlike("(Monaco|Singapore|Baku|Melbourne)"), 1).otherwise(0))
            
            # Weather and conditions (simulated based on circuit and time)
            data = data.withColumn("expected_temperature", 
                                  when(col("circuit_region") == "Asia-Pacific", 28)
                                  .when(col("circuit_region") == "Americas", 25)
                                  .otherwise(22)) \
                         .withColumn("is_wet_race", 
                                  when(col("name").rlike("(Monaco|Singapore|Belgium)"), 0.3).otherwise(0.1))
            
            # Tire strategy features
            data = data.withColumn("expected_tire_degradation", 
                                  when(col("is_street_circuit") == 1, 1.5)
                                  .when(col("circuit_region") == "Asia-Pacific", 1.2)
                                  .otherwise(1.0))
            
            # Historical performance features
            driver_window = Window.partitionBy("driverId").orderBy("raceId", "lap").rowsBetween(-5, -1)
            circuit_window = Window.partitionBy("circuitId").orderBy("raceId", "lap").rowsBetween(-10, -1)
            
            data = data.withColumn("driver_avg_lap_time_recent", 
                                  avg(col("average_lap_time")).over(driver_window)) \
                         .withColumn("circuit_avg_lap_time_recent", 
                                  avg(col("average_lap_time")).over(circuit_window)) \
                         .withColumn("lap_time_trend", 
                                  (col("average_lap_time") - col("driver_avg_lap_time_recent")) / col("driver_avg_lap_time_recent"))
            
            # Race strategy features
            data = data.withColumn("expected_pit_stops", 
                                  when(col("total_laps") <= 50, 1)
                                  .when(col("total_laps") <= 70, 2)
                                  .otherwise(3))
            
            # Handle missing values
            numeric_columns = [field.name for field in data.schema.fields 
                             if field.dataType.typeName() in ['double', 'integer', 'long', 'float']]
            
            for col_name in numeric_columns:
                data = data.withColumn(col_name, 
                                      when(col(col_name).isNull(), 0).otherwise(col(col_name)))
            
            self.logger.info("Feature engineering completed")
            return data
            
        except Exception as e:
            self.logger.error(f"Feature engineering failed: {str(e)}")
            raise
    
    def prepare_features(self, data: DataFrame) -> Tuple[DataFrame, VectorAssembler]:
        """
        Prepare features for lap time forecasting
        
        Args:
            data: Input DataFrame
            
        Returns:
            Prepared DataFrame and feature assembler
        """
        try:
            self.logger.info("Preparing features for lap time forecasting...")
            
            # Select feature columns
            feature_cols = [
                "lap", "total_laps", "race_progress", "is_early_lap", "is_late_lap",
                "driver_experience", "driver_form", "expected_temperature", "is_wet_race",
                "expected_tire_degradation", "expected_pit_stops", "driver_avg_lap_time_recent",
                "circuit_avg_lap_time_recent", "lap_time_trend", "fastest_lap_time",
                "slowest_lap_time", "pace_consistency", "position", "points"
            ]
            
            # Filter columns that exist in data
            existing_cols = [col for col in feature_cols if col in data.columns]
            self.feature_columns = existing_cols
            
            # Create feature vector
            assembler = VectorAssembler(
                inputCols=self.feature_columns,
                outputCol="features",
                handleInvalid="skip"
            )
            
            # Scale features
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features",
                withMean=True,
                withStd=True
            )
            
            # Apply transformations
            data = assembler.transform(data)
            data = scaler.fit(data).transform(data)
            
            self.logger.info(f"Prepared {len(self.feature_columns)} features for forecasting")
            return data, assembler
            
        except Exception as e:
            self.logger.error(f"Feature preparation failed: {str(e)}")
            raise
    
    def train_model(self, data: DataFrame) -> Dict[str, Any]:
        """
        Train lap time forecasting model
        
        Args:
            data: Prepared training data
            
        Returns:
            Training results and metrics
        """
        try:
            self.logger.info("Training lap time forecasting model...")
            
            # Split data
            train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
            
            # Define models to compare
            models = {
                "LinearRegression": LinearRegression(
                    featuresCol="scaled_features",
                    labelCol="average_lap_time",
                    maxIter=100,
                    regParam=0.01
                ),
                "RandomForest": RandomForestRegressor(
                    featuresCol="scaled_features",
                    labelCol="average_lap_time",
                    numTrees=100,
                    maxDepth=10,
                    seed=42
                ),
                "GradientBoosting": GBTRegressor(
                    featuresCol="scaled_features",
                    labelCol="average_lap_time",
                    maxIter=100,
                    maxDepth=5
                )
            }
            
            results = {}
            
            for model_name, model in models.items():
                self.logger.info(f"Training {model_name} model...")
                
                # Train model
                trained_model = model.fit(train_data)
                
                # Make predictions
                predictions = trained_model.transform(test_data)
                
                # Evaluate model
                evaluator = RegressionEvaluator(
                    labelCol="average_lap_time",
                    predictionCol="prediction",
                    metricName="rmse"
                )
                
                rmse = evaluator.evaluate(predictions)
                
                evaluator.setMetricName("mae")
                mae = evaluator.evaluate(predictions)
                
                evaluator.setMetricName("r2")
                r2 = evaluator.evaluate(predictions)
                
                results[model_name] = {
                    "model": trained_model,
                    "rmse": rmse,
                    "mae": mae,
                    "r2": r2,
                    "predictions": predictions
                }
                
                self.logger.info(f"{model_name} - RMSE: {rmse:.2f}, MAE: {mae:.2f}, R2: {r2:.4f}")
            
            # Select best model (lowest RMSE)
            best_model_name = min(results.keys(), key=lambda x: results[x]["rmse"])
            self.model = results[best_model_name]["model"]
            
            self.logger.info(f"Best model: {best_model_name} with RMSE: {results[best_model_name]['rmse']:.2f}")
            
            return {
                "best_model": best_model_name,
                "results": results,
                "feature_importance": self._get_feature_importance(results[best_model_name]["model"])
            }
            
        except Exception as e:
            self.logger.error(f"Model training failed: {str(e)}")
            raise
    
    def _get_feature_importance(self, model) -> Dict[str, float]:
        """Extract feature importance from trained model"""
        try:
            if hasattr(model, 'featureImportances'):
                importance = model.featureImportances.toArray()
                return dict(zip(self.feature_columns, importance))
            else:
                return {}
        except Exception as e:
            self.logger.warning(f"Could not extract feature importance: {str(e)}")
            return {}
    
    def evaluate_model(self, predictions: DataFrame) -> Dict[str, Any]:
        """
        Comprehensive model evaluation
        
        Args:
            predictions: Model predictions
            
        Returns:
            Evaluation metrics
        """
        try:
            self.logger.info("Evaluating model performance...")
            
            # Convert to Pandas for detailed analysis
            pandas_df = predictions.select("average_lap_time", "prediction").toPandas()
            
            # Calculate metrics
            rmse = np.sqrt(mean_squared_error(pandas_df["average_lap_time"], pandas_df["prediction"]))
            mae = mean_absolute_error(pandas_df["average_lap_time"], pandas_df["prediction"])
            r2 = r2_score(pandas_df["average_lap_time"], pandas_df["prediction"])
            
            # Percentage error
            pandas_df["percentage_error"] = abs(pandas_df["average_lap_time"] - pandas_df["prediction"]) / pandas_df["average_lap_time"] * 100
            mape = pandas_df["percentage_error"].mean()
            
            evaluation_results = {
                "rmse": rmse,
                "mae": mae,
                "r2": r2,
                "mape": mape,
                "mean_actual": pandas_df["average_lap_time"].mean(),
                "mean_predicted": pandas_df["prediction"].mean(),
                "std_actual": pandas_df["average_lap_time"].std(),
                "std_predicted": pandas_df["prediction"].std()
            }
            
            self.logger.info(f"Model evaluation - RMSE: {rmse:.2f}, MAE: {mae:.2f}, R2: {r2:.4f}, MAPE: {mape:.2f}%")
            
            return evaluation_results
            
        except Exception as e:
            self.logger.error(f"Model evaluation failed: {str(e)}")
            raise
    
    def predict_lap_times(self, race_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict lap times for given race conditions
        
        Args:
            race_data: Dictionary containing race features
            
        Returns:
            Prediction results
        """
        try:
            if self.model is None:
                raise ValueError("Model not trained. Please train the model first.")
            
            # Create DataFrame from input data
            input_df = self.spark.createDataFrame([race_data])
            
            # Prepare features
            input_df, _ = self.prepare_features(input_df)
            
            # Make prediction
            prediction = self.model.transform(input_df)
            
            # Extract results
            result = prediction.select("prediction").collect()[0]
            
            prediction_result = {
                "predicted_lap_time": result["prediction"],
                "predicted_lap_time_seconds": result["prediction"] / 1000.0,
                "confidence": 0.85  # Placeholder for confidence interval
            }
            
            return prediction_result
            
        except Exception as e:
            self.logger.error(f"Prediction failed: {str(e)}")
            raise
    
    def create_race_simulation(self, race_conditions: Dict[str, Any], num_laps: int = 50) -> pd.DataFrame:
        """
        Create race simulation with lap time predictions
        
        Args:
            race_conditions: Dictionary containing race conditions
            num_laps: Number of laps to simulate
            
        Returns:
            DataFrame with simulated lap times
        """
        try:
            if self.model is None:
                raise ValueError("Model not trained. Please train the model first.")
            
            simulation_results = []
            
            for lap in range(1, num_laps + 1):
                # Create lap-specific data
                lap_data = race_conditions.copy()
                lap_data["lap"] = lap
                lap_data["total_laps"] = num_laps
                lap_data["race_progress"] = lap / num_laps
                lap_data["is_early_lap"] = 1 if lap <= 5 else 0
                lap_data["is_late_lap"] = 1 if lap >= num_laps - 5 else 0
                
                # Predict lap time
                prediction = self.predict_lap_times(lap_data)
                
                simulation_results.append({
                    "lap": lap,
                    "predicted_lap_time_ms": prediction["predicted_lap_time"],
                    "predicted_lap_time_s": prediction["predicted_lap_time_seconds"],
                    "cumulative_time_s": sum([r["predicted_lap_time_seconds"] for r in simulation_results]) + prediction["predicted_lap_time_seconds"]
                })
            
            return pd.DataFrame(simulation_results)
            
        except Exception as e:
            self.logger.error(f"Race simulation failed: {str(e)}")
            raise
    
    def save_model(self, model_path: str = "models/lap_time_forecasting"):
        """
        Save trained model
        
        Args:
            model_path: Path to save model
        """
        try:
            if self.model is None:
                raise ValueError("No model to save")
            
            # Create directory if it doesn't exist
            os.makedirs(model_path, exist_ok=True)
            
            # Save Spark ML model
            self.model.save(model_path)
            
            # Save feature information
            feature_info = {
                "feature_columns": self.feature_columns,
                "target_column": self.target_column
            }
            
            joblib.dump(feature_info, f"{model_path}/feature_info.pkl")
            
            self.logger.info(f"Model saved to {model_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save model: {str(e)}")
            raise
    
    def load_model(self, model_path: str = "models/lap_time_forecasting"):
        """
        Load trained model
        
        Args:
            model_path: Path to load model from
        """
        try:
            # Load Spark ML model
            from pyspark.ml import PipelineModel
            self.model = PipelineModel.load(model_path)
            
            # Load feature information
            feature_info = joblib.load(f"{model_path}/feature_info.pkl")
            self.feature_columns = feature_info["feature_columns"]
            self.target_column = feature_info["target_column"]
            
            self.logger.info(f"Model loaded from {model_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load model: {str(e)}")
            raise
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Run complete ML pipeline
        
        Returns:
            Pipeline results
        """
        try:
            self.logger.info("Starting full lap time forecasting pipeline...")
            
            # Load and prepare data
            data = self.load_and_prepare_data()
            
            # Feature engineering
            data = self.feature_engineering(data)
            
            # Prepare features
            data, assembler = self.prepare_features(data)
            
            # Train model
            training_results = self.train_model(data)
            
            # Evaluate best model
            best_predictions = training_results["results"][training_results["best_model"]]["predictions"]
            evaluation_results = self.evaluate_model(best_predictions)
            
            # Save model
            self.save_model()
            
            results = {
                "training_results": training_results,
                "evaluation_results": evaluation_results,
                "feature_columns": self.feature_columns,
                "model_saved": True
            }
            
            self.logger.info("Lap time forecasting pipeline completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

def main():
    """Main function to run lap time forecasting"""
    try:
        # Initialize model
        forecaster = F1LapTimeForecasting()
        
        # Run full pipeline
        results = forecaster.run_full_pipeline()
        
        print("✓ Lap time forecasting pipeline completed successfully")
        print(f"Best model: {results['training_results']['best_model']}")
        print(f"Model RMSE: {results['evaluation_results']['rmse']:.2f}")
        print(f"Model MAE: {results['evaluation_results']['mae']:.2f}")
        print(f"Model R2: {results['evaluation_results']['r2']:.4f}")
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
