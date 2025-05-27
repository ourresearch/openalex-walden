from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col, when, lower, trim
from typing import Union

# Create a type that includes both DataFrame classes
DataFrameType = Union[SparkDataFrame, ConnectDataFrame]

def withColumnAt(self: DataFrameType, index: int, columnName: str, column: Column) -> DataFrameType:
    col_names = list(self.schema.names)

    # Ensure the index is within the valid range
    if not 0 <= index <= len(col_names):
        raise IndexError("Index out of valid range")

    # Check if columnName already exists in DataFrame and remove it to reinsert at new index
    if columnName in col_names:
        col_names.remove(columnName)
    
    col_names.insert(index, columnName)
    
    # Attempt to add and reorder columns
    try:
        reordered_df = self.withColumn(columnName, column).select(*col_names)
    except Exception as e:
        raise RuntimeError(f"Error reordering columns: {e}")

    return reordered_df

def normalize_boolean_column(field_name):
    """
    Returns a column expression that normalizes string representations of booleans to actual boolean types.
    Handles 'yes', 'no', 'true', 'false', 'y', 'n', 't', 'f' and their variations after trimming whitespace.
    
    Args:
    field_name (str): The name of the column to normalize.
    
    Returns:
    pyspark.sql.column.Column: A column expression for the normalized boolean values.
    """
    return (
        when(lower(trim(col(field_name))).isin("yes", "y", "true", "t"), True)
        .when(lower(trim(col(field_name))).isin("no", "n", "false", "f"), False)
        .otherwise(None)
    )

# Extend both DataFrame classes with the new method
SparkDataFrame.withColumnAt = withColumnAt
ConnectDataFrame.withColumnAt = withColumnAt