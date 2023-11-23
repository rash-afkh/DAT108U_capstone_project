import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import udf, col
from pyspark.sql.types import DateType
from pyspark.sql import DataFrame

class FileNames():
    def __init__(self) -> None:
        pass
    # Define file paths for various data sources and destinations
    airport = "data/airport-codes_csv.csv"
    immigration_sample = "data/immigration_data_sample.csv"
    city_demographics = "data/us-cities-demographics.csv"
    temperature = "data/GlobalLandTemperaturesByCity.csv"
    immigration_sas = 'data/immigration/18-83510-I94-Data-2016/*.sas7bdat'
    immigration_sas_labels = "data/I94_SAS_Labels_Descriptions.SAS"
    local_dst = "tables/"
    local_src = ''

def setup_logger() -> None:
    # Set up logging configuration
    filename = get_logger_filename()
    logging.basicConfig(
        filename=filename,
        format="%(asctime)s-%(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO
    )

def get_logger_filename() -> str:
    # Generate a unique log file name based on the current date and time
    return os.path.join(
        "logs", f"log_{datetime.now().strftime('%y%m%d-%H%M%S')}.log" 
    )

def sas_to_date(date: int) -> pd.Timestamp:
    # Convert SAS date to a Pandas Timestamp
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
    
def convert_datetime(x):
    try:
        # Convert SAS date to a Python datetime object
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None
    
udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())

def rename_columns(table: DataFrame, column_mapping: dict) -> DataFrame:
    # Rename columns in a PySpark DataFrame
    for old_column, new_column in column_mapping.items():
        table = table.withColumnRenamed(old_column, new_column)
    return table

def write_to_parquet(df: pd.DataFrame, output_path: str, table_name: str) -> None:
    # Write a Pandas DataFrame to a Parquet file
    file_path = os.path.join(output_path, table_name)
    print(f"Writing table {table_name} to {file_path}")
    
    df.write.mode("overwrite").parquet(file_path)
    print("Writing complete!")

def check_and_remove_duplicates(df: DataFrame, primary_key_column: str) -> DataFrame:
    """
    Check for duplicates in a DataFrame based on a primary key column, log if duplicates are found,
    and remove duplicates.

    Args:
        df (DataFrame): The DataFrame to check and remove duplicates from.
        primary_key_column (str): The name of the primary key column for checking duplicates.

    Returns:
        DataFrame: The DataFrame with duplicates removed.
    """
    original_count = df.count()
    
    # Check for duplicates
    duplicate_count = df.count() - df.dropDuplicates([primary_key_column]).count()
    
    if duplicate_count > 0:
        logging.warning(f"Found {duplicate_count} duplicate(s) based on '{primary_key_column}'. Original count: {original_count}. Removing duplicates...")
        
        # Remove duplicates based on the primary key column
        df = df.dropDuplicates([primary_key_column])
        
        new_count = df.count()
        logging.info(f"Removed {duplicate_count} duplicate(s). New count: {new_count}.")
    else:
        logging.info(f"No duplicates found based on '{primary_key_column}'.")
    
    return df


def remove_rows_with_missing_values(df: DataFrame, critical_columns: list) -> DataFrame:
    """
    Remove rows with missing values in specified columns of a DataFrame, log missing values, and return the cleaned DataFrame.

    Args:
        df (DataFrame): The DataFrame to check and clean.
        critical_columns (list): A list of column names to check for missing values.

    Returns:
        DataFrame: The cleaned DataFrame with missing rows removed.
    """
    for column in critical_columns:
        missing_count = df.filter(col(column).isNull()).count()
        if missing_count > 0:
            logging.info(f"Missing values in {column}: {missing_count} rows.")
            df = df.filter(col(column).isNotNull())

    return df
