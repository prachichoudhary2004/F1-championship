"""
F1 Race Intelligence Lakehouse - Driver Clustering Analysis
Enterprise-grade unsupervised learning for F1 driver segmentation
"""

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.clustering import KMeans, GaussianMixture
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import silhouette_score
import logging
from typing import Dict, List, Any, Tuple
import joblib
import os

from services.spark_session import get_spark_session
from services.logging_config import setup_logging

class F1DriverClustering:
    """
    Enterprise-grade clustering analysis for F1 driver segmentation
    Uses advanced unsupervised learning techniques
    """
    
    def __init__(self):
        self.spark = get_spark_session()
        self.logger = setup_logging("F1-Driver-Clustering")
        self.model = None
        self.feature_columns = []
        self.cluster_labels = {}
        
    def load_and_prepare_data(self) -> DataFrame:
        """
        Load and prepare data for driver clustering
        
        Returns:
            Prepared Spark DataFrame
        """
        try:
            self.logger.info("Loading data for driver clustering...")
            
            # Load gold layer tables
            driver_stats = self.spark.read.format("delta").load("data/gold/driver_statistics")
            constructor_stats = self.spark.read.format("delta").load("data/gold/constructor_rankings")
            lap_trends = self.spark.read.format("delta").load("data/gold/lap_time_trends")
            pit_efficiency = self.spark.read.format("delta").load("data/gold/pit_stop_efficiency")
            
            # Join all features
            data = driver_stats.join(constructor_stats, 
                                   (driver_stats.driverId == constructor_stats.constructorId) &
                                   (driver_stats.year == constructor_stats.year),
                                   "left") \
                               .join(lap_trends,
                                   (driver_stats.driverId == lap_trends.driverId) &
                                   (driver_stats.year == lap_trends.year),
                                   "left") \
                               .join(pit_efficiency,
                                   (driver_stats.driverId == pit_efficiency.driverId) &
                                   (driver_stats.year == pit_efficiency.year),
                                   "left")
            
            # Filter for recent years and active drivers
            data = data.filter(col("year") >= 2015) \
                       .filter(col("total_races") >= 10)  # Minimum races for meaningful analysis
            
            # Remove rows with critical missing values
            data = data.filter(col("total_points").isNotNull() & 
                             col("total_races").isNotNull())
            
            self.logger.info(f"Loaded {data.count()} records for clustering")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load and prepare data: {str(e)}")
            raise
    
    def feature_engineering(self, data: DataFrame) -> DataFrame:
        """
        Perform advanced feature engineering for clustering
        
        Args:
            data: Input DataFrame
            
        Returns:
            DataFrame with engineered features
        """
        try:
            self.logger.info("Performing feature engineering for clustering...")
            
            # Performance ratios
            data = data.withColumn("win_efficiency", 
                                  when(col("total_races") > 0,
                                      col("total_wins") / col("total_races")).otherwise(0)) \
                         .withColumn("podium_efficiency", 
                                  when(col("total_races") > 0,
                                      col("total_podiums") / col("total_races")).otherwise(0)) \
                         .withColumn("points_per_race", 
                                  when(col("total_races") > 0,
                                      col("total_points") / col("total_races")).otherwise(0))
            
            # Consistency metrics
            data = data.withColumn("finish_rate", 
                                  when(col("total_races") > 0,
                                      (col("total_races") - col("dnf_count")) / col("total_races")).otherwise(0)) \
                         .withColumn("consistency_score", 
                                  when(col("position_stddev").isNotNull(),
                                      1 - (col("position_stddev") / col("average_finish_position")))
                                  .otherwise(0))
            
            # Pace and efficiency metrics
            data = data.withColumn("pace_quality", 
                                  when(col("average_lap_time").isNotNull(),
                                      1.0 / col("average_lap_time")).otherwise(0)) \
                         .withColumn("pit_efficiency_normalized", 
                                  when(col("pit_efficiency_score").isNotNull(),
                                      col("pit_efficiency_score") / 100).otherwise(0))
            
            # Constructor impact
            data = data.withColumn("constructor_impact", 
                                  col("team_win_rate") * col("dominance_score"))
            
            # Qualifying performance
            data = data.withColumn("qualifying_strength", 
                                  when(col("qualifying_performance_score").isNotNull(),
                                      col("qualifying_performance_score") / 10).otherwise(0))
            
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
        Prepare features for clustering
        
        Args:
            data: Input DataFrame
            
        Returns:
            Prepared DataFrame and feature assembler
        """
        try:
            self.logger.info("Preparing features for clustering...")
            
            # Select clustering features
            feature_cols = [
                "total_races", "total_wins", "total_podiums", "total_points",
                "win_rate", "podium_rate", "points_per_race", "finish_rate",
                "consistency_score", "win_efficiency", "podium_efficiency",
                "pace_quality", "pit_efficiency_normalized", "constructor_impact",
                "qualifying_strength", "average_finish_position"
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
            
            self.logger.info(f"Prepared {len(self.feature_columns)} features for clustering")
            return data, assembler
            
        except Exception as e:
            self.logger.error(f"Feature preparation failed: {str(e)}")
            raise
    
    def find_optimal_clusters(self, data: DataFrame, max_clusters: int = 10) -> Dict[int, float]:
        """
        Find optimal number of clusters using elbow method and silhouette score
        
        Args:
            data: Prepared data
            max_clusters: Maximum number of clusters to test
            
        Returns:
            Dictionary of cluster counts and their scores
        """
        try:
            self.logger.info("Finding optimal number of clusters...")
            
            cluster_scores = {}
            evaluator = ClusteringEvaluator(featuresCol="scaled_features", metricName="silhouette")
            
            for k in range(2, max_clusters + 1):
                # Train KMeans model
                kmeans = KMeans(featuresCol="scaled_features", k=k, seed=42)
                model = kmeans.fit(data)
                predictions = model.transform(data)
                
                # Calculate silhouette score
                silhouette = evaluator.evaluate(predictions)
                cluster_scores[k] = silhouette
                
                self.logger.info(f"K={k}: Silhouette Score = {silhouette:.4f}")
            
            # Find optimal k (highest silhouette score)
            optimal_k = max(cluster_scores.keys(), key=lambda x: cluster_scores[x])
            
            self.logger.info(f"Optimal number of clusters: {optimal_k} (Silhouette: {cluster_scores[optimal_k]:.4f})")
            
            return cluster_scores
            
        except Exception as e:
            self.logger.error(f"Failed to find optimal clusters: {str(e)}")
            raise
    
    def train_clustering_model(self, data: DataFrame, n_clusters: int = 5) -> Dict[str, Any]:
        """
        Train clustering model
        
        Args:
            data: Prepared data
            n_clusters: Number of clusters
            
        Returns:
            Training results
        """
        try:
            self.logger.info(f"Training clustering model with {n_clusters} clusters...")
            
            # Train KMeans model
            kmeans = KMeans(
                featuresCol="scaled_features",
                k=n_clusters,
                seed=42,
                initMode="k-means||",
                maxIter=100
            )
            
            self.model = kmeans.fit(data)
            predictions = self.model.transform(data)
            
            # Evaluate model
            evaluator = ClusteringEvaluator(featuresCol="scaled_features", metricName="silhouette")
            silhouette_score = evaluator.evaluate(predictions)
            
            # Get cluster centers
            cluster_centers = self.model.clusterCenters()
            
            # Analyze clusters
            cluster_analysis = self._analyze_clusters(predictions, cluster_centers)
            
            self.logger.info(f"Clustering completed with silhouette score: {silhouette_score:.4f}")
            
            return {
                "model": self.model,
                "predictions": predictions,
                "silhouette_score": silhouette_score,
                "cluster_centers": cluster_centers,
                "cluster_analysis": cluster_analysis
            }
            
        except Exception as e:
            self.logger.error(f"Clustering model training failed: {str(e)}")
            raise
    
    def _analyze_clusters(self, predictions: DataFrame, cluster_centers) -> Dict[str, Any]:
        """
        Analyze clustering results
        
        Args:
            predictions: DataFrame with cluster predictions
            cluster_centers: Cluster center coordinates
            
        Returns:
            Cluster analysis results
        """
        try:
            self.logger.info("Analyzing clusters...")
            
            # Convert to Pandas for analysis
            pandas_df = predictions.select("*").toPandas()
            
            # Cluster statistics
            cluster_stats = {}
            for cluster_id in range(len(cluster_centers)):
                cluster_data = pandas_df[pandas_df['prediction'] == cluster_id]
                
                stats = {
                    "size": len(cluster_data),
                    "percentage": len(cluster_data) / len(pandas_df) * 100,
                    "avg_total_wins": cluster_data['total_wins'].mean(),
                    "avg_total_points": cluster_data['total_points'].mean(),
                    "avg_win_rate": cluster_data['win_rate'].mean(),
                    "avg_consistency": cluster_data['consistency_score'].mean(),
                    "avg_finish_rate": cluster_data['finish_rate'].mean()
                }
                
                cluster_stats[cluster_id] = stats
            
            # Create cluster labels based on characteristics
            cluster_labels = {}
            for cluster_id, stats in cluster_stats.items():
                if stats['avg_win_rate'] > 0.2:
                    label = "Elite Champions"
                elif stats['avg_win_rate'] > 0.1:
                    label = "Race Winners"
                elif stats['avg_win_rate'] > 0.05:
                    label = "Podium Contenders"
                elif stats['avg_consistency'] > 0.7:
                    label = "Consistent Finishers"
                else:
                    label = "Development Drivers"
                
                cluster_labels[cluster_id] = label
            
            self.cluster_labels = cluster_labels
            
            return {
                "cluster_stats": cluster_stats,
                "cluster_labels": cluster_labels,
                "total_drivers": len(pandas_df)
            }
            
        except Exception as e:
            self.logger.error(f"Cluster analysis failed: {str(e)}")
            raise
    
    def get_driver_segments(self, predictions: DataFrame) -> DataFrame:
        """
        Get driver segments with labels
        
        Args:
            predictions: DataFrame with cluster predictions
            
        Returns:
            DataFrame with driver segments
        """
        try:
            # Add cluster labels
            from pyspark.sql.types import StringType
            
            # Create label mapping
            label_mapping = create_map([lit(int(k)) | lit(v) for k, v in self.cluster_labels.items()])
            
            # Add labels to predictions
            labeled_predictions = predictions.withColumn("cluster_label", 
                                                     label_mapping[col("prediction").cast(IntegerType())])
            
            # Select relevant columns
            driver_segments = labeled_predictions.select(
                "driverId", "forename", "surname", "nationality", "year",
                "prediction", "cluster_label",
                "total_races", "total_wins", "total_points", "win_rate",
                "consistency_score", "finish_rate"
            )
            
            return driver_segments
            
        except Exception as e:
            self.logger.error(f"Failed to get driver segments: {str(e)}")
            raise
    
    def visualize_clusters(self, predictions: DataFrame, save_path: str = "notebooks/ml/cluster_visualizations"):
        """
        Create visualizations for clustering results
        
        Args:
            predictions: DataFrame with cluster predictions
            save_path: Path to save visualizations
        """
        try:
            self.logger.info("Creating cluster visualizations...")
            
            # Create directory if it doesn't exist
            os.makedirs(save_path, exist_ok=True)
            
            # Convert to Pandas for visualization
            pandas_df = predictions.select("*").toPandas()
            
            # Add cluster labels
            pandas_df['cluster_label'] = pandas_df['prediction'].map(self.cluster_labels)
            
            # 1. Cluster distribution
            plt.figure(figsize=(12, 8))
            sns.countplot(data=pandas_df, x='cluster_label', palette='viridis')
            plt.title('Driver Cluster Distribution')
            plt.xlabel('Cluster Segment')
            plt.ylabel('Number of Drivers')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(f"{save_path}/cluster_distribution.png", dpi=300, bbox_inches='tight')
            plt.close()
            
            # 2. Win Rate vs Consistency scatter plot
            plt.figure(figsize=(12, 8))
            sns.scatterplot(data=pandas_df, x='win_rate', y='consistency_score', 
                           hue='cluster_label', palette='viridis', s=100)
            plt.title('Driver Segments: Win Rate vs Consistency')
            plt.xlabel('Win Rate')
            plt.ylabel('Consistency Score')
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            plt.savefig(f"{save_path}/win_rate_vs_consistency.png", dpi=300, bbox_inches='tight')
            plt.close()
            
            # 3. Points vs Races scatter plot
            plt.figure(figsize=(12, 8))
            sns.scatterplot(data=pandas_df, x='total_races', y='total_points', 
                           hue='cluster_label', palette='viridis', s=100)
            plt.title('Driver Segments: Total Points vs Total Races')
            plt.xlabel('Total Races')
            plt.ylabel('Total Points')
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.tight_layout()
            plt.savefig(f"{save_path}/points_vs_races.png", dpi=300, bbox_inches='tight')
            plt.close()
            
            # 4. Cluster characteristics heatmap
            cluster_stats = {}
            for cluster_id in pandas_df['prediction'].unique():
                cluster_data = pandas_df[pandas_df['prediction'] == cluster_id]
                cluster_stats[cluster_id] = {
                    'Win Rate': cluster_data['win_rate'].mean(),
                    'Consistency': cluster_data['consistency_score'].mean(),
                    'Finish Rate': cluster_data['finish_rate'].mean(),
                    'Points per Race': cluster_data['points_per_race'].mean()
                }
            
            stats_df = pd.DataFrame(cluster_stats).T
            stats_df.index = [self.cluster_labels.get(i, f'Cluster {i}') for i in stats_df.index]
            
            plt.figure(figsize=(12, 8))
            sns.heatmap(stats_df, annot=True, cmap='viridis', center=0)
            plt.title('Cluster Characteristics Heatmap')
            plt.tight_layout()
            plt.savefig(f"{save_path}/cluster_characteristics.png", dpi=300, bbox_inches='tight')
            plt.close()
            
            self.logger.info(f"Visualizations saved to {save_path}")
            
        except Exception as e:
            self.logger.error(f"Visualization failed: {str(e)}")
            raise
    
    def save_model(self, model_path: str = "models/driver_clustering"):
        """
        Save trained clustering model
        
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
            
            # Save cluster information
            cluster_info = {
                "feature_columns": self.feature_columns,
                "cluster_labels": self.cluster_labels
            }
            
            joblib.dump(cluster_info, f"{model_path}/cluster_info.pkl")
            
            self.logger.info(f"Clustering model saved to {model_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save model: {str(e)}")
            raise
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Run complete clustering pipeline
        
        Returns:
            Pipeline results
        """
        try:
            self.logger.info("Starting full driver clustering pipeline...")
            
            # Load and prepare data
            data = self.load_and_prepare_data()
            
            # Feature engineering
            data = self.feature_engineering(data)
            
            # Prepare features
            data, assembler = self.prepare_features(data)
            
            # Find optimal clusters
            cluster_scores = self.find_optimal_clusters(data)
            optimal_k = max(cluster_scores.keys(), key=lambda x: cluster_scores[x])
            
            # Train clustering model
            training_results = self.train_clustering_model(data, optimal_k)
            
            # Get driver segments
            driver_segments = self.get_driver_segments(training_results["predictions"])
            
            # Create visualizations
            self.visualize_clusters(training_results["predictions"])
            
            # Save model
            self.save_model()
            
            results = {
                "optimal_clusters": optimal_k,
                "cluster_scores": cluster_scores,
                "training_results": training_results,
                "driver_segments": driver_segments,
                "model_saved": True
            }
            
            self.logger.info("Driver clustering pipeline completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

def main():
    """Main function to run driver clustering"""
    try:
        # Initialize clustering
        clustering = F1DriverClustering()
        
        # Run full pipeline
        results = clustering.run_full_pipeline()
        
        print("✓ Driver clustering pipeline completed successfully")
        print(f"Optimal clusters: {results['optimal_clusters']}")
        print(f"Silhouette score: {results['training_results']['silhouette_score']:.4f}")
        
        # Print cluster information
        cluster_analysis = results['training_results']['cluster_analysis']
        print("\nDriver Segments:")
        for cluster_id, label in cluster_analysis['cluster_labels'].items():
            stats = cluster_analysis['cluster_stats'][cluster_id]
            print(f"  {label}: {stats['size']} drivers ({stats['percentage']:.1f}%)")
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
