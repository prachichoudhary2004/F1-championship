"""
F1 Race Intelligence Lakehouse - Data Quality Framework
Enterprise-grade data quality validation and monitoring
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import json

@dataclass
class QualityCheck:
    """Data quality check configuration"""
    name: str
    description: str
    check_type: str  # 'null_check', 'duplicate_check', 'range_check', 'schema_check', 'freshness_check'
    column: Optional[str] = None
    condition: Optional[str] = None
    threshold: Optional[float] = None
    expected_schema: Optional[StructType] = None

@dataclass
class QualityResult:
    """Data quality check result"""
    check_name: str
    status: str  # 'PASS', 'FAIL', 'WARNING'
    details: Dict[str, Any]
    timestamp: datetime
    error_message: Optional[str] = None

class F1DataQualityManager:
    """
    Enterprise data quality management for F1 Race Intelligence Lakehouse
    Provides comprehensive validation for medallion architecture layers
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        self.quality_results: List[QualityResult] = []
    
    def run_quality_checks(self, df: DataFrame, table_name: str, checks: List[QualityCheck]) -> List[QualityResult]:
        """
        Run comprehensive data quality checks on DataFrame
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table being checked
            checks: List of quality checks to perform
            
        Returns:
            List of quality check results
        """
        self.logger.info(f"Running {len(checks)} quality checks on table: {table_name}")
        
        for check in checks:
            try:
                result = self._execute_check(df, check)
                self.quality_results.append(result)
                
                if result.status == 'FAIL':
                    self.logger.error(f"Quality check failed: {check.name} - {result.error_message}")
                elif result.status == 'WARNING':
                    self.logger.warning(f"Quality check warning: {check.name} - {result.error_message}")
                else:
                    self.logger.info(f"Quality check passed: {check.name}")
                    
            except Exception as e:
                error_result = QualityResult(
                    check_name=check.name,
                    status='FAIL',
                    details={'error': str(e)},
                    timestamp=datetime.now(),
                    error_message=f"Check execution failed: {str(e)}"
                )
                self.quality_results.append(error_result)
                self.logger.error(f"Quality check execution failed: {check.name} - {str(e)}")
        
        return self.quality_results
    
    def _execute_check(self, df: DataFrame, check: QualityCheck) -> QualityResult:
        """Execute individual quality check"""
        
        if check.check_type == 'null_check':
            return self._null_check(df, check)
        elif check.check_type == 'duplicate_check':
            return self._duplicate_check(df, check)
        elif check.check_type == 'range_check':
            return self._range_check(df, check)
        elif check.check_type == 'schema_check':
            return self._schema_check(df, check)
        elif check.check_type == 'freshness_check':
            return self._freshness_check(df, check)
        elif check.check_type == 'row_count_check':
            return self._row_count_check(df, check)
        else:
            raise ValueError(f"Unsupported check type: {check.check_type}")
    
    def _null_check(self, df: DataFrame, check: QualityCheck) -> QualityResult:
        """Check for null values in specified column"""
        if not check.column:
            raise ValueError("Column must be specified for null check")
        
        total_count = df.count()
        null_count = df.filter(col(check.column).isNull()).count()
        null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
        
        status = 'PASS'
        error_message = None
        
        if check.threshold and null_percentage > check.threshold:
            status = 'FAIL'
            error_message = f"Null percentage ({null_percentage:.2f}%) exceeds threshold ({check.threshold}%)"
        elif null_percentage > 0:
            status = 'WARNING'
            error_message = f"Found {null_count} null values ({null_percentage:.2f}%)"
        
        return QualityResult(
            check_name=check.name,
            status=status,
            details={
                'total_rows': total_count,
                'null_count': null_count,
                'null_percentage': null_percentage,
                'column': check.column
            },
            timestamp=datetime.now(),
            error_message=error_message
        )
    
    def _duplicate_check(self, df: DataFrame, check: QualityCheck) -> QualityResult:
        """Check for duplicate records"""
        if not check.column:
            # Check for complete row duplicates
            total_count = df.count()
            distinct_count = df.distinct().count()
            duplicate_count = total_count - distinct_count
        else:
            # Check for duplicates in specified column
            total_count = df.count()
            distinct_count = df.select(check.column).distinct().count()
            duplicate_count = total_count - distinct_count
        
        duplicate_percentage = (duplicate_count / total_count) * 100 if total_count > 0 else 0
        
        status = 'PASS'
        error_message = None
        
        if check.threshold and duplicate_percentage > check.threshold:
            status = 'FAIL'
            error_message = f"Duplicate percentage ({duplicate_percentage:.2f}%) exceeds threshold ({check.threshold}%)"
        elif duplicate_count > 0:
            status = 'WARNING'
            error_message = f"Found {duplicate_count} duplicate records ({duplicate_percentage:.2f}%)"
        
        return QualityResult(
            check_name=check.name,
            status=status,
            details={
                'total_rows': total_count,
                'duplicate_count': duplicate_count,
                'duplicate_percentage': duplicate_percentage,
                'column': check.column
            },
            timestamp=datetime.now(),
            error_message=error_message
        )
    
    def _range_check(self, df: DataFrame, check: QualityCheck) -> QualityResult:
        """Check if values are within expected range"""
        if not check.column or not check.condition:
            raise ValueError("Column and condition must be specified for range check")
        
        try:
            # Apply the condition to filter valid records
            valid_df = df.filter(check.condition)
            total_count = df.count()
            valid_count = valid_df.count()
            invalid_count = total_count - valid_count
            invalid_percentage = (invalid_count / total_count) * 100 if total_count > 0 else 0
            
            status = 'PASS'
            error_message = None
            
            if check.threshold and invalid_percentage > check.threshold:
                status = 'FAIL'
                error_message = f"Invalid value percentage ({invalid_percentage:.2f}%) exceeds threshold ({check.threshold}%)"
            elif invalid_count > 0:
                status = 'WARNING'
                error_message = f"Found {invalid_count} invalid values ({invalid_percentage:.2f}%)"
            
            return QualityResult(
                check_name=check.name,
                status=status,
                details={
                    'total_rows': total_count,
                    'valid_count': valid_count,
                    'invalid_count': invalid_count,
                    'invalid_percentage': invalid_percentage,
                    'column': check.column,
                    'condition': check.condition
                },
                timestamp=datetime.now(),
                error_message=error_message
            )
            
        except Exception as e:
            return QualityResult(
                check_name=check.name,
                status='FAIL',
                details={'error': str(e)},
                timestamp=datetime.now(),
                error_message=f"Range check failed: {str(e)}"
            )
    
    def _schema_check(self, df: DataFrame, check: QualityCheck) -> QualityResult:
        """Check DataFrame schema against expected schema"""
        if not check.expected_schema:
            raise ValueError("Expected schema must be specified for schema check")
        
        actual_schema = df.schema
        expected_schema = check.expected_schema
        
        actual_fields = set(field.name for field in actual_schema.fields)
        expected_fields = set(field.name for field in expected_schema.fields)
        
        missing_fields = expected_fields - actual_fields
        extra_fields = actual_fields - expected_fields
        
        status = 'PASS'
        error_message = None
        
        if missing_fields:
            status = 'FAIL'
            error_message = f"Missing fields: {list(missing_fields)}"
        elif extra_fields:
            status = 'WARNING'
            error_message = f"Extra fields found: {list(extra_fields)}"
        
        return QualityResult(
            check_name=check.name,
            status=status,
            details={
                'actual_field_count': len(actual_fields),
                'expected_field_count': len(expected_fields),
                'missing_fields': list(missing_fields),
                'extra_fields': list(extra_fields)
            },
            timestamp=datetime.now(),
            error_message=error_message
        )
    
    def _freshness_check(self, df: DataFrame, check: QualityCheck) -> QualityResult:
        """Check data freshness based on timestamp column"""
        if not check.column:
            raise ValueError("Column must be specified for freshness check")
        
        try:
            # Get the latest timestamp
            latest_timestamp = df.agg(max(check.column).alias("max_timestamp")).collect()[0]["max_timestamp"]
            
            if latest_timestamp is None:
                return QualityResult(
                    check_name=check.name,
                    status='FAIL',
                    details={'latest_timestamp': None},
                    timestamp=datetime.now(),
                    error_message="No timestamp values found"
                )
            
            # Calculate age in hours
            current_time = datetime.now()
            age_hours = (current_time - latest_timestamp).total_seconds() / 3600
            
            status = 'PASS'
            error_message = None
            
            if check.threshold and age_hours > check.threshold:
                status = 'FAIL'
                error_message = f"Data age ({age_hours:.2f} hours) exceeds threshold ({check.threshold} hours)"
            elif age_hours > 24:  # Warning if data is older than 24 hours
                status = 'WARNING'
                error_message = f"Data is {age_hours:.2f} hours old"
            
            return QualityResult(
                check_name=check.name,
                status=status,
                details={
                    'latest_timestamp': str(latest_timestamp),
                    'age_hours': age_hours,
                    'column': check.column
                },
                timestamp=datetime.now(),
                error_message=error_message
            )
            
        except Exception as e:
            return QualityResult(
                check_name=check.name,
                status='FAIL',
                details={'error': str(e)},
                timestamp=datetime.now(),
                error_message=f"Freshness check failed: {str(e)}"
            )
    
    def _row_count_check(self, df: DataFrame, check: QualityCheck) -> QualityResult:
        """Check if row count meets minimum threshold"""
        row_count = df.count()
        
        status = 'PASS'
        error_message = None
        
        if check.threshold and row_count < check.threshold:
            status = 'FAIL'
            error_message = f"Row count ({row_count}) below minimum threshold ({check.threshold})"
        
        return QualityResult(
            check_name=check.name,
            status=status,
            details={'row_count': row_count, 'minimum_threshold': check.threshold},
            timestamp=datetime.now(),
            error_message=error_message
        )
    
    def get_quality_summary(self) -> Dict[str, Any]:
        """Get summary of all quality checks"""
        if not self.quality_results:
            return {'message': 'No quality checks run yet'}
        
        total_checks = len(self.quality_results)
        passed_checks = len([r for r in self.quality_results if r.status == 'PASS'])
        failed_checks = len([r for r in self.quality_results if r.status == 'FAIL'])
        warning_checks = len([r for r in self.quality_results if r.status == 'WARNING'])
        
        return {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'warning_checks': warning_checks,
            'pass_rate': (passed_checks / total_checks) * 100 if total_checks > 0 else 0,
            'checks': [
                {
                    'name': r.check_name,
                    'status': r.status,
                    'timestamp': str(r.timestamp),
                    'error_message': r.error_message
                }
                for r in self.quality_results
            ]
        }
    
    def clear_results(self):
        """Clear all quality check results"""
        self.quality_results.clear()

# Predefined quality checks for F1 data
F1_BRONZE_CHECKS = [
    QualityCheck(
        name="races_null_check",
        description="Check for null values in races table",
        check_type="null_check",
        column="raceId",
        threshold=0.0
    ),
    QualityCheck(
        name="races_row_count",
        description="Check minimum row count for races table",
        check_type="row_count_check",
        threshold=100
    ),
    QualityCheck(
        name="drivers_null_check",
        description="Check for null values in drivers table",
        check_type="null_check",
        column="driverId",
        threshold=0.0
    ),
    QualityCheck(
        name="lap_times_range_check",
        description="Check lap times are positive",
        check_type="range_check",
        column="milliseconds",
        condition="milliseconds > 0",
        threshold=1.0
    )
]

F1_SILVER_CHECKS = [
    QualityCheck(
        name="performance_null_check",
        description="Check for null values in performance table",
        check_type="null_check",
        column="driverId",
        threshold=0.0
    ),
    QualityCheck(
        name="performance_freshness",
        description="Check data freshness",
        check_type="freshness_check",
        column="ingestion_timestamp",
        threshold=48  # 48 hours
    )
]

F1_GOLD_CHECKS = [
    QualityCheck(
        name="driver_statistics_null_check",
        description="Check for null values in driver statistics",
        check_type="null_check",
        column="driverId",
        threshold=0.0
    ),
    QualityCheck(
        name="rankings_range_check",
        description="Check championship rankings are valid",
        check_type="range_check",
        column="position",
        condition="position >= 1 AND position <= 50",
        threshold=0.0
    )
]
