import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from pyspark.sql import DataFrame

class FileNames():
    def __init__(self) -> None:
        pass
    airport = "data/airport-codes_csv.csv"
    immigration_sample = "data/immigration_data_sample.csv"
    city_demographics = "data/us-cities-demographics.csv"
    temperature = "data/GlobalLandTemperaturesByCity.csv"
    immigration_sas = 'data/immigration/18-83510-I94-Data-2016/*.sas7bdat'
    immigration_sas_labels = "data/I94_SAS_Labels_Descriptions.SAS"
    local_dst = "tables/"
    local_src = ''

def setup_logger() -> None:
    filename = get_logger_filename()
    logging.basicConfig(
        filename=filename,
        format="%(asctime)s-%(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO
    )

def get_logger_filename() -> str:
    return os.path.join(
        "logs", f"log_{datetime.now().strftime('%y%m%d-%H%M%S')}.log" 
    )

def sas_to_date(date: int) -> pd.Timestamp:
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
    
def convert_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None
    
udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())

def rename_columns(table: DataFrame, column_mapping: dict) -> DataFrame:
    for old_column, new_column in column_mapping.items():
        table = table.withColumnRenamed(old_column, new_column)
    return table


def write_to_parquet(df: pd.DataFrame, output_path: str, table_name: str) -> None:
    file_path = os.path.join(output_path, table_name)
    print(f"Writing table {table_name} to {file_path}")
    
    df.write.mode("overwrite").parquet(file_path)
    print("Writing complete!")