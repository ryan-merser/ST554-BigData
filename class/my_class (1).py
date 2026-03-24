from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd
 
 
class SparkDataCheck:
    """
    A data quality class for Spark SQL style data frames
    """
 
    def __init__(self, df: DataFrame):
        """
        Initialize SparkDataCheck with a Spark DataFrame.
        """
        self.df = df
        
    # Create methods for creating a new instance of the class while reading in data
    @classmethod
    def from_pandas(cls, spark, pandas_df):
        """
        Create a SparkDataCheck instance from a pandas DataFrame.
        """
        df = spark.createDataFrame(pandas_df)
        return cls(df)
    
    @classmethod
    def from_csv(cls, spark, path: str):
        """
        Create a SparkDataCheck instance by reading a CSV file.
        """
        df = spark.read.load(path, format="csv", header=True, inferSchema=True)
        return cls(df)
    
    #############################
    # Validation methods
    def check_numeric_range(self, column: str, lower: str = None, upper: str = None):
        """
        Append Boolean column indicating whether numeric values fall within bounds.
        """
        # Require at least one bound
        if lower is None and upper is None:
            print("check_numeric_range: please supply at least one of 'lower' or 'upper'.")
            return self
        
        # Column type validation
        numeric_types = {"float", "int", "long", "bigint", "double", "integer", "short", "tinyint"}
        col_type = dict(self.df.dtypes).get(column, "")
        if col_type.lower() not in numeric_types:
            print(
                f"check_numeric_bounds: column '{column}' has type '{col_type}' "
                f"which is not numeric. DataFrame is unchanged."
            )
            return self
        
        result_col = f"{column}_in_bounds"
        col_expr = F.col(column)
        
        if lower is not None and upper is not None:
            check_expr = col_expr.between(lower, upper)
        elif lower is not None:
            check_expr = col_expr >= lower
        else:
            check_expr = col_expr <= upper
            
        # Return null when input is null
        result_expr = F.when(col_expr.isNull(), None).otherwise(check_expr)
 
        self.df = self.df.withColumn(result_col, result_expr)
        return self
    
    def check_levels(self, column: str, levels: list):
        """
        Check whether each value in a string column belongs to a user-specified set of levels.
        """
        col_type = dict(self.df.dtypes).get(column, "")
        if col_type.lower() != "string":
            print(
                f"check_categorical_levels: column '{column}' has type '{col_type}' "
                f"which is not a string. DataFrame is unchanged."
            )
            return self
 
        result_col = f"{column}_in_levels"
        col_expr = F.col(column)
 
        # Return null when input is null
        result_expr = F.when(col_expr.isNull(), None).otherwise(col_expr.isin(levels))
 
        self.df = self.df.withColumn(result_col, result_expr)
        return self
    
    def check_missing(self, column: str):
        """ 
        Check whether each value in a column is Null
        """
        result_col = f"{column}_is_missing"
        self.df = self.df.withColumn(result_col, F.col(column).isNull())
        return self
        
        
    ##############################
    # Summarization methods
    def summarize_numeric(self, column: str = None, group_by: str = None):
        """
        Report the min and max of a numeric column (or all numeric columns).
        Returns results as a pandas DataFrame.
        """
        numeric_types = {"float", "int", "long", "bigint", "double", "integer", "short", "tinyint"}
        dtype_map = dict(self.df.dtypes)
        
        # Summary for single column
        if column is not None:
            col_type = dtype_map.get(column, "")
            if col_type.lower() not in numeric_types:
                print(
                    f"summarize_numeric: column '{column}' has type '{col_type}' "
                    f"which is not numeric. Returning None."
                )
                return None
            
            agg_exprs = [F.min(F.col(f"`{column}`")).alias(f"{column}_min"),
                         F.max(F.col(f"`{column}`")).alias(f"{column}_max")]
            
            if group_by:
                result = self.df.groupBy(group_by).agg(*agg_exprs).orderBy(group_by)
            else:
                result = self.df.agg(*agg_exprs)
 
            return result.toPandas()

        # Summary for all numeric columns
        numeric_cols = [c for c, t in self.df.dtypes if t.lower() in numeric_types]
        
        if not numeric_cols:
            print("summarize_numeric: no numeric columns found in the DataFrame.")
            return None
        
        if group_by:
            # Summarize each numeric column separately then merge on the group key
            def summarize_one(col_name):
                return (
                    self.df.groupBy(group_by)
                    .agg(F.min(F.col(f"`{col_name}`")).alias(f"{col_name}_min"),
                         F.max(F.col(f"`{col_name}`")).alias(f"{col_name}_max"))
                    .orderBy(group_by)
                    .toPandas()
                )
            
            partial_dfs = [summarize_one(c) for c in numeric_cols]
            return reduce(lambda left, right: pd.merge(left, right, on=group_by), partial_dfs)
        else:
            agg_exprs = [expr
                         for c in numeric_cols
                         for expr in (F.min(F.col(f"`{c}`")).alias(f"{c}_min"),
                                      F.max(F.col(f"`{c}`")).alias(f"{c}_max"))]
            return self.df.agg(*agg_exprs).toPandas()
        
        
    def summarize_counts(self, column: str, column2: str = None):
        """ 
        Report value counts for one or two string columns.
        Returns results as a pandas dataframe.
        """
        dtype_map = dict(self.df.dtypes)
        
        # Validate first column
        col_type = dtype_map.get(column, "")
        if col_type.lower() != "string":
            print(
                f"summarize_counts: column '{column}' has type '{col_type}' "
                f"which is not a string column."
            )
            return None
 
        # Validate second column
        if column2 is not None:
            col2_type = dtype_map.get(column2, "")
            if col2_type.lower() != "string":
                print(
                    f"summarize_counts: column '{column2}' has type '{col2_type}' "
                    f"which is not a string column."
                )
                return None
 
        group_cols = [column] if column2 is None else [column, column2]
 
        result = (
            self.df.groupBy(*group_cols)
            .agg(F.count("*").alias("count"))
            .orderBy(*group_cols)
        )
 
        return result.toPandas()
            