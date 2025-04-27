# monitoring/cloudwatch_monitor.py

import logging
import boto3
import time
from datetime import datetime, timedelta
from config.aws_config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    CLOUDWATCH_NAMESPACE,
    METRIC_DIMENSIONS
)

logger = logging.getLogger(__name__)

class CloudWatchMonitor:
    """
    Class for monitoring ETL processes using AWS CloudWatch.
    
    This class provides methods to publish metrics, set alarms,
    and retrieve metrics from CloudWatch.
    """
    
    def __init__(self, config=None):
        """
        Initialize the CloudWatch monitor.
        
        Args:
            config (dict, optional): Configuration parameters for monitoring
        """
        self.config = config or {}
        self.namespace = self.config.get('namespace', CLOUDWATCH_NAMESPACE)
        self.dimensions = self.config.get('dimensions', METRIC_DIMENSIONS)
        
        # Create CloudWatch client
        self.cloudwatch = boto3.client(
            'cloudwatch',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        logger.info(f"Initialized CloudWatch monitor with namespace: {self.namespace}")
    
    def put_metric(self, metric_name, value, unit='Count', dimensions=None):
        """
        Publish a metric to CloudWatch.
        
        Args:
            metric_name (str): Name of the metric
            value (float/int): Metric value
            unit (str): Unit of the metric
            dimensions (list/dict, optional): Custom dimensions for the metric
            
        Returns:
            bool: True if the metric was published successfully, False otherwise
        """
        try:
            # Use custom dimensions if provided, otherwise use default
            metric_dimensions = dimensions or self.dimensions
            
            # Convert dict to list of dicts if needed
            if isinstance(metric_dimensions, dict):
                metric_dimensions = [{'Name': k, 'Value': v} for k, v in metric_dimensions.items()]
            
            # Publish metric
            response = self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[
                    {
                        'MetricName': metric_name,
                        'Value': value,
                        'Unit': unit,
                        'Dimensions': metric_dimensions,
                        'Timestamp': datetime.now()
                    }
                ]
            )
            
            logger.info(f"Published metric {metric_name}: {value} {unit}")
            return True
        except Exception as e:
            logger.error(f"Error publishing metric {metric_name}: {str(e)}")
            return False
    
    def put_execution_time(self, process_name, start_time, end_time=None):
        """
        Publish execution time metric for a process.
        
        Args:
            process_name (str): Name of the process
            start_time (datetime): Start time of the process
            end_time (datetime, optional): End time of the process, defaults to now
            
        Returns:
            bool: True if the metric was published successfully, False otherwise
        """
        if end_time is None:
            end_time = datetime.now()
        
        # Calculate duration in seconds
        duration = (end_time - start_time).total_seconds()
        
        # Define dimensions
        dimensions = self.dimensions.copy() if isinstance(self.dimensions, dict) else {}
        dimensions['ProcessName'] = process_name
        
        # Publish metric
        return self.put_metric(
            metric_name='ExecutionTime',
            value=duration,
            unit='Seconds',
            dimensions=dimensions
        )
    
    def put_record_count(self, process_name, count, status='Processed'):
        """
        Publish record count metric for a process.
        
        Args:
            process_name (str): Name of the process
            count (int): Number of records
            status (str): Status of the records (e.g., 'Processed', 'Failed', 'Skipped')
            
        Returns:
            bool: True if the metric was published successfully, False otherwise
        """
        # Define dimensions
        dimensions = self.dimensions.copy() if isinstance(self.dimensions, dict) else {}
        dimensions['ProcessName'] = process_name
        dimensions['Status'] = status
        
        # Publish metric
        return self.put_metric(
            metric_name='RecordCount',
            value=count,
            unit='Count',
            dimensions=dimensions
        )
    
    def put_data_quality_score(self, process_name, score):
        """
        Publish data quality score metric for a process.
        
        Args:
            process_name (str): Name of the process
            score (float): Data quality score (0-100)
            
        Returns:
            bool: True if the metric was published successfully, False otherwise
        """
        # Define dimensions
        dimensions = self.dimensions.copy() if isinstance(self.dimensions, dict) else {}
        dimensions['ProcessName'] = process_name
        
        # Publish metric
        return self.put_metric(
            metric_name='DataQualityScore',
            value=score,
            unit='Percent',
            dimensions=dimensions
        )
    
    def put_success_flag(self, process_name, success=True):
        """
        Publish success flag metric for a process.
        
        Args:
            process_name (str): Name of the process
            success (bool): Whether the process succeeded
            
        Returns:
            bool: True if the metric was published successfully, False otherwise
        """
        # Define dimensions
        dimensions = self.dimensions.copy() if isinstance(self.dimensions, dict) else {}
        dimensions['ProcessName'] = process_name
        
        # Publish metric
        return self.put_metric(
            metric_name='Success',
            value=1 if success else 0,
            unit='None',
            dimensions=dimensions
        )
    
    def create_alarm(self, alarm_name, metric_name, threshold, comparison_operator,
                     evaluation_periods=1, period=60, statistic='Average',
                     actions_enabled=True, alarm_actions=None):
        """
        Create a CloudWatch alarm for a metric.
        
        Args:
            alarm_name (str): Name of the alarm
            metric_name (str): Name of the metric to monitor
            threshold (float): Threshold value for the alarm
            comparison_operator (str): Comparison operator for the alarm
            evaluation_periods (int): Number of periods to evaluate
            period (int): Evaluation period in seconds
            statistic (str): Statistic to use for the alarm
            actions_enabled (bool): Whether to enable alarm actions
            alarm_actions (list): Actions to take when the alarm is triggered
            
        Returns:
            bool: True if the alarm was created successfully, False otherwise
        """
        try:
            # Create alarm
            response = self.cloudwatch.put_metric_alarm(
                AlarmName=alarm_name,
                AlarmDescription=f"Alarm for {metric_name}",
                ActionsEnabled=actions_enabled,
                AlarmActions=alarm_actions or [],
                MetricName=metric_name,
                Namespace=self.namespace,
                Statistic=statistic,
                Dimensions=[{'Name': k, 'Value': v} for k, v in self.dimensions.items()],
                Period=period,
                EvaluationPeriods=evaluation_periods,
                Threshold=threshold,
                ComparisonOperator=comparison_operator
            )
            
            logger.info(f"Created alarm {alarm_name} for metric {metric_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating alarm {alarm_name}: {str(e)}")
            return False