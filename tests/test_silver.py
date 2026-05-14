"""
F1 Race Intelligence Lakehouse - Silver Layer Tests
Unit tests for data cleansing and transformation logic
"""

import pytest
from pyspark.sql import SparkSession
from pipelines.silver_pipeline import F1SilverPipeline
import os

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing"""
    return SparkSession.builder \
        .appName("F1-Silver-Tests") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def test_silver_pipeline_init(spark):
    """Test pipeline initialization"""
    pipeline = F1SilverPipeline()
    assert pipeline.spark is not None

def test_silver_data_paths():
    """Check if silver data directories exist"""
    assert os.path.exists("data/silver")
    assert os.path.isdir("data/silver")

# Note: Integration tests would require bronze data to be present
