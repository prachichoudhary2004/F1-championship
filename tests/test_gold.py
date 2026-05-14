"""
F1 Race Intelligence Lakehouse - Gold Layer Tests
Unit tests for business intelligence and analytics logic
"""

import pytest
from pyspark.sql import SparkSession
from pipelines.gold_pipeline import F1GoldPipeline
import os

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing"""
    return SparkSession.builder \
        .appName("F1-Gold-Tests") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def test_gold_pipeline_init(spark):
    """Test pipeline initialization"""
    pipeline = F1GoldPipeline()
    assert pipeline.spark is not None

def test_gold_data_paths():
    """Check if gold data directories exist"""
    assert os.path.exists("data/gold")
    assert os.path.isdir("data/gold")
