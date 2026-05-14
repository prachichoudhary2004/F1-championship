"""
F1 Race Intelligence Lakehouse - Bronze Layer Tests
Unit tests for data ingestion and raw layer processing
"""

import pytest
from pyspark.sql import SparkSession
from pipelines.bronze_pipeline import F1BronzePipeline
import os

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing"""
    return SparkSession.builder \
        .appName("F1-Tests") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def test_bronze_pipeline_init(spark):
    """Test pipeline initialization"""
    pipeline = F1BronzePipeline()
    assert pipeline.spark is not None
    assert pipeline.delta_utils is not None

def test_races_ingestion_path():
    """Check if races data exists"""
    assert os.path.exists("data/raw/races.csv")

def test_drivers_ingestion_path():
    """Check if drivers data exists"""
    assert os.path.exists("data/raw/drivers.json")

# Note: More complex tests would involve creating sample data and running the pipeline
# which requires a full Delta Lake environment setup.
