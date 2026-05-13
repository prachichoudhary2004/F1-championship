"""
F1 Race Intelligence Lakehouse - Monitoring Service
Enterprise-grade monitoring and alerting for production pipelines
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import json

@dataclass
class PipelineMetrics:
    """Pipeline execution metrics"""
    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str
    rows_processed: int
    tables_processed: List[str]
    error_message: Optional[str] = None
    execution_time_seconds: Optional[float] = None

@dataclass
class TableMetrics:
    """Table-specific metrics"""
    table_name: str
    row_count: int
    size_mb: Optional[float]
    partition_count: Optional[int]
    last_updated: datetime
    quality_score: float

class F1MonitoringService:
    """
    Enterprise monitoring service for F1 Race Intelligence Lakehouse
    Tracks pipeline performance, data quality, and system health
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        self.pipeline_metrics: List[PipelineMetrics] = []
        self.table_metrics: List[TableMetrics] = []
    
    def start_pipeline_monitoring(self, pipeline_name: str) -> str:
        """
        Start monitoring a pipeline execution
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Pipeline execution ID
        """
        execution_id = f"{pipeline_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        metrics = PipelineMetrics(
            pipeline_name=pipeline_name,
            start_time=datetime.now(),
            end_time=None,
            status="RUNNING",
            rows_processed=0,
            tables_processed=[]
        )
        
        self.pipeline_metrics.append(metrics)
        
        self.logger.info(f"Started monitoring pipeline: {pipeline_name} (ID: {execution_id})")
        return execution_id
    
    def end_pipeline_monitoring(self, pipeline_name: str, status: str = "SUCCESS", error_message: Optional[str] = None):
        """
        End monitoring for a pipeline execution
        
        Args:
            pipeline_name: Name of the pipeline
            status: Final status (SUCCESS, FAILED, PARTIAL)
            error_message: Error message if failed
        """
        for metrics in self.pipeline_metrics:
            if metrics.pipeline_name == pipeline_name and metrics.status == "RUNNING":
                metrics.end_time = datetime.now()
                metrics.status = status
                metrics.error_message = error_message
                
                if metrics.start_time and metrics.end_time:
                    metrics.execution_time_seconds = (metrics.end_time - metrics.start_time).total_seconds()
                
                self.logger.info(f"Completed monitoring pipeline: {pipeline_name} - Status: {status}")
                if error_message:
                    self.logger.error(f"Pipeline error: {error_message}")
                break
    
    def record_table_metrics(self, table_name: str, df: DataFrame):
        """
        Record metrics for a processed table
        
        Args:
            table_name: Name of the table
            df: DataFrame containing the table data
        """
        try:
            row_count = df.count()
            
            # Estimate table size (rough calculation)
            size_mb = self._estimate_table_size(df)
            
            # Get partition count if applicable
            partition_count = self._get_partition_count(table_name)
            
            metrics = TableMetrics(
                table_name=table_name,
                row_count=row_count,
                size_mb=size_mb,
                partition_count=partition_count,
                last_updated=datetime.now(),
                quality_score=self._calculate_quality_score(table_name)
            )
            
            self.table_metrics.append(metrics)
            
            # Update pipeline metrics
            for pipeline_metrics in self.pipeline_metrics:
                if pipeline_metrics.status == "RUNNING":
                    pipeline_metrics.rows_processed += row_count
                    if table_name not in pipeline_metrics.tables_processed:
                        pipeline_metrics.tables_processed.append(table_name)
                    break
            
            self.logger.info(f"Recorded metrics for table: {table_name} ({row_count:,} rows)")
            
        except Exception as e:
            self.logger.error(f"Failed to record metrics for table {table_name}: {str(e)}")
    
    def get_pipeline_summary(self, pipeline_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary of pipeline executions
        
        Args:
            pipeline_name: Optional specific pipeline name
            
        Returns:
            Pipeline summary metrics
        """
        relevant_metrics = self.pipeline_metrics
        if pipeline_name:
            relevant_metrics = [m for m in self.pipeline_metrics if m.pipeline_name == pipeline_name]
        
        if not relevant_metrics:
            return {"message": "No pipeline metrics found"}
        
        total_executions = len(relevant_metrics)
        successful_executions = len([m for m in relevant_metrics if m.status == "SUCCESS"])
        failed_executions = len([m for m in relevant_metrics if m.status == "FAILED"])
        
        avg_execution_time = None
        completed_metrics = [m for m in relevant_metrics if m.execution_time_seconds is not None]
        if completed_metrics:
            avg_execution_time = sum(m.execution_time_seconds for m in completed_metrics) / len(completed_metrics)
        
        total_rows_processed = sum(m.rows_processed for m in relevant_metrics)
        
        return {
            "pipeline_name": pipeline_name or "ALL",
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "failed_executions": failed_executions,
            "success_rate": (successful_executions / total_executions) * 100 if total_executions > 0 else 0,
            "average_execution_time_seconds": avg_execution_time,
            "total_rows_processed": total_rows_processed,
            "recent_executions": [
                {
                    "pipeline_name": m.pipeline_name,
                    "start_time": str(m.start_time),
                    "status": m.status,
                    "execution_time_seconds": m.execution_time_seconds,
                    "rows_processed": m.rows_processed,
                    "tables_processed": m.tables_processed
                }
                for m in sorted(relevant_metrics, key=lambda x: x.start_time, reverse=True)[:5]
            ]
        }
    
    def get_table_summary(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary of table metrics
        
        Args:
            table_name: Optional specific table name
            
        Returns:
            Table summary metrics
        """
        relevant_metrics = self.table_metrics
        if table_name:
            relevant_metrics = [m for m in self.table_metrics if m.table_name == table_name]
        
        if not relevant_metrics:
            return {"message": "No table metrics found"}
        
        # Get latest metrics for each table
        latest_metrics = {}
        for metric in relevant_metrics:
            if metric.table_name not in latest_metrics or metric.last_updated > latest_metrics[metric.table_name].last_updated:
                latest_metrics[metric.table_name] = metric
        
        total_rows = sum(m.row_count for m in latest_metrics.values())
        total_size_mb = sum(m.size_mb or 0 for m in latest_metrics.values())
        avg_quality_score = sum(m.quality_score for m in latest_metrics.values()) / len(latest_metrics) if latest_metrics else 0
        
        return {
            "table_name": table_name or "ALL",
            "total_tables": len(latest_metrics),
            "total_rows": total_rows,
            "total_size_mb": total_size_mb,
            "average_quality_score": avg_quality_score,
            "tables": [
                {
                    "table_name": m.table_name,
                    "row_count": m.row_count,
                    "size_mb": m.size_mb,
                    "partition_count": m.partition_count,
                    "last_updated": str(m.last_updated),
                    "quality_score": m.quality_score
                }
                for m in latest_metrics.values()
            ]
        }
    
    def check_pipeline_health(self) -> Dict[str, Any]:
        """
        Check overall pipeline health and return status
        
        Returns:
            Health check results
        """
        # Check recent pipeline executions (last 24 hours)
        cutoff_time = datetime.now() - timedelta(hours=24)
        recent_executions = [m for m in self.pipeline_metrics if m.start_time >= cutoff_time]
        
        if not recent_executions:
            return {
                "status": "UNKNOWN",
                "message": "No pipeline executions in the last 24 hours",
                "recommendations": ["Check if pipelines are running on schedule"]
            }
        
        recent_failures = len([m for m in recent_executions if m.status == "FAILED"])
        failure_rate = (recent_failures / len(recent_executions)) * 100
        
        health_status = "HEALTHY"
        recommendations = []
        
        if failure_rate > 20:
            health_status = "UNHEALTHY"
            recommendations.append("High failure rate detected - investigate pipeline errors")
        elif failure_rate > 10:
            health_status = "WARNING"
            recommendations.append("Elevated failure rate - monitor closely")
        
        # Check for long-running pipelines
        long_running = [m for m in recent_executions if m.execution_time_seconds and m.execution_time_seconds > 3600]  # > 1 hour
        if long_running:
            recommendations.append("Some pipelines taking longer than expected - consider optimization")
        
        # Check data freshness
        stale_tables = self._check_data_freshness()
        if stale_tables:
            recommendations.append(f"Tables not updated recently: {', '.join(stale_tables)}")
        
        return {
            "status": health_status,
            "failure_rate_24h": failure_rate,
            "recent_executions": len(recent_executions),
            "recent_failures": recent_failures,
            "recommendations": recommendations,
            "last_check": str(datetime.now())
        }
    
    def export_metrics(self, file_path: str):
        """
        Export all metrics to JSON file
        
        Args:
            file_path: Path to export file
        """
        try:
            export_data = {
                "export_timestamp": str(datetime.now()),
                "pipeline_metrics": [
                    {
                        "pipeline_name": m.pipeline_name,
                        "start_time": str(m.start_time),
                        "end_time": str(m.end_time) if m.end_time else None,
                        "status": m.status,
                        "rows_processed": m.rows_processed,
                        "tables_processed": m.tables_processed,
                        "execution_time_seconds": m.execution_time_seconds,
                        "error_message": m.error_message
                    }
                    for m in self.pipeline_metrics
                ],
                "table_metrics": [
                    {
                        "table_name": m.table_name,
                        "row_count": m.row_count,
                        "size_mb": m.size_mb,
                        "partition_count": m.partition_count,
                        "last_updated": str(m.last_updated),
                        "quality_score": m.quality_score
                    }
                    for m in self.table_metrics
                ]
            }
            
            with open(file_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            self.logger.info(f"Metrics exported to: {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to export metrics: {str(e)}")
    
    def _estimate_table_size(self, df: DataFrame) -> float:
        """Estimate table size in MB"""
        try:
            # Get schema and estimate row size
            schema = df.schema
            estimated_row_size = 0
            
            for field in schema.fields:
                if field.dataType == IntegerType():
                    estimated_row_size += 4
                elif field.dataType == LongType():
                    estimated_row_size += 8
                elif field.dataType == FloatType():
                    estimated_row_size += 4
                elif field.dataType == DoubleType():
                    estimated_row_size += 8
                elif field.dataType == StringType():
                    estimated_row_size += 50  # Average string length
                elif field.dataType == TimestampType():
                    estimated_row_size += 8
                else:
                    estimated_row_size += 20  # Default for complex types
            
            # Sample a few rows to get better estimate
            sample_count = min(1000, df.count())
            if sample_count > 0:
                estimated_size_mb = (estimated_row_size * sample_count) / (1024 * 1024)
                return estimated_size_mb
            
            return 0.0
            
        except Exception:
            return 0.0
    
    def _get_partition_count(self, table_name: str) -> Optional[int]:
        """Get partition count for a Delta table"""
        try:
            # This is a simplified approach - in production you'd query Delta log
            return None
        except Exception:
            return None
    
    def _calculate_quality_score(self, table_name: str) -> float:
        """Calculate quality score for a table (0-100)"""
        # This would integrate with data quality results
        # For now, return a default score
        return 95.0
    
    def _check_data_freshness(self, hours_threshold: int = 24) -> List[str]:
        """Check for tables that haven't been updated recently"""
        stale_tables = []
        cutoff_time = datetime.now() - timedelta(hours=hours_threshold)
        
        for metric in self.table_metrics:
            if metric.last_updated < cutoff_time:
                stale_tables.append(metric.table_name)
        
        return stale_tables

# Global monitoring service instance
monitoring_service = None

def get_monitoring_service(spark: SparkSession) -> F1MonitoringService:
    """Get or create monitoring service instance"""
    global monitoring_service
    if monitoring_service is None:
        monitoring_service = F1MonitoringService(spark)
    return monitoring_service
