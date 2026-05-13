"""
F1 Race Intelligence Lakehouse - Race Winner Prediction Model
Enterprise-grade machine learning for F1 race outcome prediction
"""

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, classification_report
import logging
from typing import Dict, List, Any, Tuple
import joblib
import os

from services.spark_session import get_spark_session
from services.logging_config import setup_logging

class F1RaceWinnerPrediction:
    """
    Enterprise-grade machine learning model for F1 race winner prediction
    Uses advanced feature engineering and ensemble methods
    """
    
    def __init__(self):
        self.spark = get_spark_session()
        self.logger = setup_logging("F1-Winner-Prediction")
        self.model = None
        self.feature_columns = []
        self.label_column = "is_winner"
        
    def load_and_prepare_data(self) -> DataFrame:
        """
        Load and prepare data for race winner prediction
        
        Returns:
            Prepared Spark DataFrame
        """
        try:
            self.logger.info("Loading data for race winner prediction...")
            
            # Load gold layer tables
            driver_stats = self.spark.read.format("delta").load("data/gold/driver_statistics")
            constructor_stats = self.spark.read.format("delta").load("data/gold/constructor_rankings")
            lap_trends = self.spark.read.format("delta").load("data/gold/lap_time_trends")
            pit_efficiency = self.spark.read.format("delta").load("data/gold/pit_stop_efficiency")
            qualifying_vs_race = self.spark.read.format("delta").load("data/gold/qualifying_vs_race")
            
            # Join all features
            data = driver_stats.join(constructor_stats, 
                                   (driver_stats.driverId == qualifying_vs_race.driverId) &
                                   (driver_stats.year == qualifying_vs_race.year),
                                   "left") \
                               .join(lap_trends,
                                   (driver_stats.driverId == lap_trends.driverId) &
                                   (driver_stats.year == lap_trends.year),
                                   "left") \
                               .join(pit_efficiency,
                                   (driver_stats.driverId == pit_efficiency.driverId) &
                                   (driver_stats.year == pit_efficiency.year),
                                   "left") \
                               .join(qualifying_vs_race,
                                   (driver_stats.driverId == qualifying_vs_race.driverId) &
                                   (driver_stats.year == qualifying_vs_race.year),
                                   "left")
            
            # Create target variable
            data = data.withColumn(self.label_column, 
                                  when(col("total_wins") > 0, 1).otherwise(0))
            
            # Filter for recent years (more relevant data)
            data = data.filter(col("year") >= 2010)
            
            # Remove rows with missing target
            data = data.filter(col(self.label_column).isNotNull())
            
            self.logger.info(f"Loaded {data.count()} records for training")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load and prepare data: {str(e)}")
            raise
    
    def feature_engineering(self, data: DataFrame) -> DataFrame:
        """
        Perform advanced feature engineering
        
        Args:
            data: Input DataFrame
            
        Returns:
            DataFrame with engineered features
        """
        try:
            self.logger.info("Performing feature engineering...")
            
            # Create performance ratios
            data = data.withColumn("win_to_podium_ratio", 
                                  when(col("total_podiums") > 0, 
                                      col("total_wins") / col("total_podiums")).otherwise(0)) \
                         .withColumn("points_per_win", 
                                  when(col("total_wins") > 0,
                                      col("total_points") / col("total_wins")).otherwise(0))
            
            # Create consistency features
            data = data.withColumn("performance_consistency", 
                                  1 - (col("position_stddev") / col("average_finish_position")))
            
            # Create qualifying performance features
            data = data.withColumn("qualifying_to_race_correlation",
                                  when(col("qualifying_position").isNotNull() & col("position").isNotNull(),
                                      1.0 / (abs(col("qualifying_position") - col("position")) + 1))
                                  .otherwise(0))
            
            # Create constructor dominance features
            data = data.withColumn("constructor_dominance",
                                  col("team_win_rate") * col("dominance_score"))
            
            # Create pace efficiency features
            data = data.withColumn("pace_efficiency_score",
                                  when(col("average_lap_time").isNotNull(),
                                      1.0 / col("average_lap_time")).otherwise(0))
            
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
        Prepare features for machine learning
        
        Args:
            data: Input DataFrame
            
        Returns:
            Prepared DataFrame and feature assembler
        """
        try:
            self.logger.info("Preparing features for ML...")
            
            # Select feature columns
            feature_cols = [
                "total_races", "total_wins", "total_podiums", "total_points",
                "average_finish_position", "win_rate", "podium_rate", "dnf_rate",
                "points_per_race", "finish_rate", "consistency_score",
                "team_win_rate", "team_podium_rate", "points_per_race_constructor",
                "dominance_score", "fastest_lap_time", "average_lap_time",
                "pace_consistency", "pit_efficiency_score", "qualifying_performance_score",
                "win_to_podium_ratio", "performance_consistency", "constructor_dominance"
            ]
            
            # Filter columns that exist in the data
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
            
            self.logger.info(f"Prepared {len(self.feature_columns)} features")
            return data, assembler
            
        except Exception as e:
            self.logger.error(f"Feature preparation failed: {str(e)}")
            raise
    
    def train_model(self, data: DataFrame) -> Dict[str, Any]:
        """
        Train race winner prediction model
        
        Args:
            data: Prepared training data
            
        Returns:
            Training results and metrics
        """
        try:
            self.logger.info("Training race winner prediction model...")
            
            # Split data
            train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)
            
            # Define models to compare
            models = {
                "RandomForest": RandomForestClassifier(
                    featuresCol="scaled_features",
                    labelCol=self.label_column,
                    numTrees=100,
                    maxDepth=10,
                    seed=42
                ),
                "GradientBoosting": GBTClassifier(
                    featuresCol="scaled_features",
                    labelCol=self.label_column,
                    maxIter=100,
                    maxDepth=5
                ),
                "LogisticRegression": LogisticRegression(
                    featuresCol="scaled_features",
                    labelCol=self.label_column,
                    maxIter=100,
                    regParam=0.01
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
                evaluator = BinaryClassificationEvaluator(
                    labelCol=self.label_column,
                    rawPredictionCol="rawPrediction",
                    metricName="areaUnderROC"
                )
                
                auc = evaluator.evaluate(predictions)
                
                # Additional metrics
                accuracy_evaluator = MulticlassClassificationEvaluator(
                    labelCol=self.label_column,
                    predictionCol="prediction",
                    metricName="accuracy"
                )
                
                accuracy = accuracy_evaluator.evaluate(predictions)
                
                results[model_name] = {
                    "model": trained_model,
                    "auc": auc,
                    "accuracy": accuracy,
                    "predictions": predictions
                }
                
                self.logger.info(f"{model_name} - AUC: {auc:.4f}, Accuracy: {accuracy:.4f}")
            
            # Select best model
            best_model_name = max(results.keys(), key=lambda x: results[x]["auc"])
            self.model = results[best_model_name]["model"]
            
            self.logger.info(f"Best model: {best_model_name} with AUC: {results[best_model_name]['auc']:.4f}")
            
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
            pandas_df = predictions.select(self.label_column, "prediction", "probability").toPandas()
            
            # Basic metrics
            accuracy = (pandas_df[self.label_column] == pandas_df['prediction']).mean()
            
            # Confusion matrix
            cm = confusion_matrix(pandas_df[self.label_column], pandas_df['prediction'])
            
            # Classification report
            report = classification_report(pandas_df[self.label_column], pandas_df['prediction'])
            
            # ROC AUC
            evaluator = BinaryClassificationEvaluator(
                labelCol=self.label_column,
                rawPredictionCol="rawPrediction",
                metricName="areaUnderROC"
            )
            auc = evaluator.evaluate(predictions)
            
            evaluation_results = {
                "accuracy": accuracy,
                "auc": auc,
                "confusion_matrix": cm.tolist(),
                "classification_report": report,
                "precision": cm[1,1] / (cm[1,1] + cm[0,1]) if (cm[1,1] + cm[0,1]) > 0 else 0,
                "recall": cm[1,1] / (cm[1,1] + cm[1,0]) if (cm[1,1] + cm[1,0]) > 0 else 0,
                "f1_score": 2 * cm[1,1] / (2 * cm[1,1] + cm[0,1] + cm[1,0]) if (2 * cm[1,1] + cm[0,1] + cm[1,0]) > 0 else 0
            }
            
            self.logger.info(f"Model evaluation - Accuracy: {accuracy:.4f}, AUC: {auc:.4f}")
            
            return evaluation_results
            
        except Exception as e:
            self.logger.error(f"Model evaluation failed: {str(e)}")
            raise
    
    def predict_race_winner(self, race_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict race winner for given race data
        
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
            result = prediction.select("prediction", "probability").collect()[0]
            
            prediction_result = {
                "predicted_winner": bool(result["prediction"]),
                "probability": result["probability"][1] if len(result["probability"]) > 1 else 0.0,
                "confidence": max(result["probability"])
            }
            
            return prediction_result
            
        except Exception as e:
            self.logger.error(f"Prediction failed: {str(e)}")
            raise
    
    def save_model(self, model_path: str = "models/race_winner_prediction"):
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
                "label_column": self.label_column
            }
            
            joblib.dump(feature_info, f"{model_path}/feature_info.pkl")
            
            self.logger.info(f"Model saved to {model_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save model: {str(e)}")
            raise
    
    def load_model(self, model_path: str = "models/race_winner_prediction"):
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
            self.label_column = feature_info["label_column"]
            
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
            self.logger.info("Starting full race winner prediction pipeline...")
            
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
            
            self.logger.info("Race winner prediction pipeline completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

def main():
    """Main function to run race winner prediction"""
    try:
        # Initialize model
        predictor = F1RaceWinnerPrediction()
        
        # Run full pipeline
        results = predictor.run_full_pipeline()
        
        print("✓ Race winner prediction pipeline completed successfully")
        print(f"Best model: {results['training_results']['best_model']}")
        print(f"Model AUC: {results['evaluation_results']['auc']:.4f}")
        print(f"Model Accuracy: {results['evaluation_results']['accuracy']:.4f}")
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
