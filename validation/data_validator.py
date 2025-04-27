# validation/data_validator.py

import pandas as pd
import numpy as np
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
import json
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Class for validating data using PySpark.
    
    This class provides methods to validate various aspects of data quality,
    including completeness, consistency, and conformity to business rules.
    """
    
    def __init__(self, config=None):
        """
        Initialize the data validator.
        
        Args:
            config (dict, optional): Configuration parameters for validation
        """
        self.config = config or {}
        self.spark = None
        self.init_spark()
        logger.info("Initialized DataValidator")
    
    def init_spark(self):
        """
        Initialize Spark session.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        try:
            # Use existing Spark session if available
            self.spark = SparkSession.builder \
                .appName("FinancialDataValidator") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.driver.memory", self.config.get("spark_driver_memory", "2g")) \
                .getOrCreate()
            
            logger.info(f"Initialized Spark session: {self.spark.version}")
            return True
        except Exception as e:
            logger.error(f"Error initializing Spark: {str(e)}")
            return False
    
    def convert_pandas_to_spark(self, df):
        """
        Convert pandas DataFrame to Spark DataFrame.
        
        Args:
            df (pandas.DataFrame): Pandas DataFrame
            
        Returns:
            pyspark.sql.DataFrame: Spark DataFrame or None if conversion failed
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for validation")
            return None
        
        try:
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = self.spark.createDataFrame(df)
            logger.info(f"Converted pandas DataFrame to Spark DataFrame with {spark_df.count()} rows")
            return spark_df
        except Exception as e:
            logger.error(f"Error converting pandas DataFrame to Spark DataFrame: {str(e)}")
            return None
    
    def validate_data(self, df, validation_rules=None):
        """
        Validate data against a set of rules.
        
        Args:
            df (pandas.DataFrame): Data to validate
            validation_rules (dict, optional): Custom validation rules
            
        Returns:
            dict: Validation results
        """
        if df.empty:
            logger.warning("Empty DataFrame provided for validation")
            return {"valid": False, "errors": ["Empty DataFrame"]}
        
        # Convert pandas DataFrame to Spark DataFrame
        spark_df = self.convert_pandas_to_spark(df)
        if spark_df is None:
            return {"valid": False, "errors": ["Failed to convert to Spark DataFrame"]}
        
        # Get validation rules (use provided rules or default)
        rules = validation_rules or self.config.get("validation_rules", self._get_default_rules(df))
        
        # Run validations
        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "row_count": spark_df.count(),
            "column_count": len(spark_df.columns),
            "validations": []
        }
        
        all_valid = True
        
        # Apply each validation rule
        for rule in rules:
            rule_result = self._apply_validation_rule(spark_df, rule)
            validation_results["validations"].append(rule_result)
            
            if not rule_result["valid"]:
                all_valid = False
        
        validation_results["valid"] = all_valid
        
        # Calculate overall data quality score (percentage of passed validations)
        passed_validations = sum(1 for rule_result in validation_results["validations"] if rule_result["valid"])
        total_validations = len(validation_results["validations"])
        validation_results["quality_score"] = round((passed_validations / total_validations) * 100, 2) if total_validations > 0 else 0
        
        logger.info(f"Data validation complete. Quality score: {validation_results['quality_score']}%")
        return validation_results
    
    def _apply_validation_rule(self, spark_df, rule):
        """
        Apply a single validation rule to the DataFrame.
        
        Args:
            spark_df (pyspark.sql.DataFrame): Spark DataFrame
            rule (dict): Validation rule
            
        Returns:
            dict: Result of applying the rule
        """
        rule_type = rule.get("type")
        rule_params = rule.get("params", {})
        
        result = {
            "rule_id": rule.get("id", "unknown"),
            "rule_type": rule_type,
            "description": rule.get("description", ""),
            "valid": False,
            "details": {}
        }
        
        try:
            if rule_type == "completeness":
                result = self._validate_completeness(spark_df, rule_params, result)
            elif rule_type == "uniqueness":
                result = self._validate_uniqueness(spark_df, rule_params, result)
            elif rule_type == "value_range":
                result = self._validate_value_range(spark_df, rule_params, result)
            else:
                result["details"] = {"error": f"Unknown rule type: {rule_type}"}
                logger.warning(f"Unknown validation rule type: {rule_type}")
        
        except Exception as e:
            result["valid"] = False
            result["details"] = {"error": str(e)}
            logger.error(f"Error applying validation rule {rule.get('id', '')}: {str(e)}")
        
        return result
    
    def _validate_completeness(self, spark_df, params, result):
        """
        Validate completeness of specified columns.
        
        Args:
            spark_df (pyspark.sql.DataFrame): Spark DataFrame
            params (dict): Parameters for completeness validation
            result (dict): Result template
            
        Returns:
            dict: Updated result
        """
        columns = params.get("columns", spark_df.columns)
        threshold = params.get("threshold", 1.0)  # Default: require 100% completeness
        
        # Calculate completeness for each column
        completeness_results = {}
        all_valid = True
        
        for column in columns:
            if column in spark_df.columns:
                # Count non-null values
                total_rows = spark_df.count()
                non_null_count = spark_df.filter(~isnull(col(column)) & ~isnan(col(column))).count() if column in spark_df.columns else 0
                completeness_ratio = non_null_count / total_rows if total_rows > 0 else 0
                
                # Check if completeness meets threshold
                is_valid = completeness_ratio >= threshold
                if not is_valid:
                    all_valid = False
                
                completeness_results[column] = {
                    "completeness_ratio": round(completeness_ratio, 4),
                    "missing_count": total_rows - non_null_count,
                    "valid": is_valid
                }
            else:
                all_valid = False
                completeness_results[column] = {
                    "error": "Column not found",
                    "valid": False
                }
        
        result["valid"] = all_valid
        result["details"] = completeness_results
        
        return result
    
    def _validate_uniqueness(self, spark_df, params, result):
        """
        Validate uniqueness of specified columns.
        
        Args:
            spark_df (pyspark.sql.DataFrame): Spark DataFrame
            params (dict): Parameters for uniqueness validation
            result (dict): Result template
            
        Returns:
            dict: Updated result
        """
        columns = params.get("columns", [])
        if not columns:
            result["valid"] = False
            result["details"] = {"error": "No columns specified for uniqueness validation"}
            return result
        
        # Convert single column to list if needed
        if isinstance(columns, str):
            columns = [columns]
        
        # Check if all columns exist
        missing_columns = [col for col in columns if col not in spark_df.columns]
        if missing_columns:
            result["valid"] = False
            result["details"] = {"error": f"Columns not found: {missing_columns}"}
            return result
        
        # Calculate uniqueness
        total_rows = spark_df.count()
        unique_rows = spark_df.select(columns).distinct().count()
        uniqueness_ratio = unique_rows / total_rows if total_rows > 0 else 1.0
        
        # Check against threshold
        threshold = params.get("threshold", 1.0)  # Default: require 100% uniqueness
        is_valid = uniqueness_ratio >= threshold
        
        result["valid"] = is_valid
        result["details"] = {
            "columns": columns,
            "uniqueness_ratio": round(uniqueness_ratio, 4),
            "duplicate_count": total_rows - unique_rows,
            "threshold": threshold
        }
        
        return result
    
    def _validate_value_range(self, spark_df, params, result):
        """
        Validate that values in a column fall within a specified range.
        
        Args:
            spark_df (pyspark.sql.DataFrame): Spark DataFrame
            params (dict): Parameters for range validation
            result (dict): Result template
            
        Returns:
            dict: Updated result
        """
        column = params.get("column")
        min_value = params.get("min")
        max_value = params.get("max")
        
        # Check if column exists
        if column not in spark_df.columns:
            result["valid"] = False
            result["details"] = {"error": f"Column not found: {column}"}
            return result
        
        # Count values outside the range
        out_of_range_count = 0
        
        if min_value is not None and max_value is not None:
            out_of_range_count = spark_df.filter(
                (col(column) < min_value) | (col(column) > max_value)
            ).count()
        elif min_value is not None:
            out_of_range_count = spark_df.filter(col(column) < min_value).count()
        elif max_value is not None:
            out_of_range_count = spark_df.filter(col(column) > max_value).count()
        else:
            result["valid"] = False
            result["details"] = {"error": "Neither min nor max value specified"}
            return result
       
        # Calculate compliance ratio
        total_rows = spark_df.count()
        compliance_ratio = 1.0 - (out_of_range_count / total_rows) if total_rows > 0 else 1.0
        
        # Check against threshold
        threshold = params.get("threshold", 1.0)  # Default: require 100% compliance
        is_valid = compliance_ratio >= threshold
        
        result["valid"] = is_valid
        result["details"] = {
            "column": column,
            "min_value": min_value,
            "max_value": max_value,
            "compliance_ratio": round(compliance_ratio, 4),
            "out_of_range_count": out_of_range_count,
            "threshold": threshold
        }
        
        return result
    
    def _get_default_rules(self, df):
        """
        Generate default validation rules based on DataFrame structure.
        
        Args:
            df (pandas.DataFrame): DataFrame for which to generate rules
            
        Returns:
            list: List of default validation rules
        """
        rules = []
        
        # Always check completeness of all columns
        rules.append({
            "id": "default_completeness",
            "type": "completeness",
            "description": "Check completeness of all columns",
            "params": {
                "columns": df.columns.tolist(),
                "threshold": 0.95  # Allow up to 5% missing values
            }
        })
        
        # For numeric columns, add range validations
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        for col in numeric_cols:
            # Skip date-like numeric columns
            if 'date' in col.lower() or 'time' in col.lower():
                continue
                
            # Get percentiles for reasonable range bounds (allow outliers)
            try:
                min_val = float(df[col].quantile(0.001))  # 0.1 percentile
                max_val = float(df[col].quantile(0.999))  # 99.9 percentile
                
                rules.append({
                    "id": f"default_range_{col}",
                    "type": "value_range",
                    "description": f"Check that {col} values are within reasonable range",
                    "params": {
                        "column": col,
                        "min": min_val,
                        "max": max_val,
                        "threshold": 0.99  # Allow up to 1% outliers
                    }
                })
            except Exception as e:
                logger.warning(f"Could not generate range validation for {col}: {str(e)}")
        
        return rules
    
    def save_validation_results(self, results, file_path):
        """
        Save validation results to a file.
        
        Args:
            results (dict): Validation results
            file_path (str): Path where to save the results
            
        Returns:
            bool: True if saving was successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Save results as JSON
            with open(file_path, 'w') as f:
                json.dump(results, f, indent=2)
            
            logger.info(f"Saved validation results to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving validation results: {str(e)}")
            return False
    
    def stop_spark(self):
        """
        Stop the Spark session.
        """
        if self.spark:
            self.spark.stop()
            logger.info("Stopped Spark session")