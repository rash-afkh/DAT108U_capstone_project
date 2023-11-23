# Import necessary libraries and modules
import configparser
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month, upper, to_date, udf
from pyspark.sql.functions import monotonically_increasing_id

# Import custom functions and configurations
from tools import (
    setup_logger, 
    rename_columns, 
    FileNames, 
    udf_datetime_from_sas, 
    check_and_remove_duplicates,
    remove_rows_with_missing_values,
)

# Initialize logger for logging purposes
setup_logger()

# Read configurations from 'config.cfg' file
class ReadConfigs():
    config = configparser.ConfigParser()
    config.read('config.cfg', encoding='utf-8-sig')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
    DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

# Create a Spark session for data processing
def create_spark_session() -> SparkSession:
    """
    Create a Spark session for data processing.
    
    Returns:
        SparkSession: An instance of SparkSession.
    """
    logging.info("Creating Spark session")
    spark = SparkSession.builder \
        .config("spark.jars.packages",
                "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport().getOrCreate()
    return spark

# Process immigration data
def process_immigration_data(spark: SparkSession, src: str, dst: str, use_sample_data=False) -> None:
    """
    Process immigration data and save it in parquet format.
    
    Args:
        spark (SparkSession): The Spark session instance.
        src (str): Source location of the data.
        dst (str): Destination location to save processed data.
        use_sample_data (bool, optional): Flag to use sample data. Defaults to False.
    """
    logging.info("Processing immigration data")

    # Load immigration data from either sample or SAS format
    if use_sample_data:
        immigration_data_path = os.path.join(src + FileNames.immigration_sample)
        df = spark.read.option('header', True).csv(immigration_data_path)
    else:
        immigration_data_path = os.path.join(src + FileNames.immigration_sas)
        df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data_path)

    # Process fact_immigration table
    logging.info("Processing fact_immigration")
    logging.debug("Extracting columns to create fact_immigration table")
    fact_immigration_columns_to_select = ['cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', 'arrdate', 'depdate', 'i94mode', 'i94visa']
    fact_immigration = df.select(fact_immigration_columns_to_select).distinct() \
        .withColumn("immigration_id", monotonically_increasing_id())

    # Data Quality Check - Integrity Constraints (Primary Key)
    logging.info("Performing data quality check - Integrity Constraints (Primary Key)")
    assert fact_immigration.count() == fact_immigration.select("immigration_id").distinct().count(), \
        "Data quality check failed: Duplicate primary key values found in fact_immigration."

    # Data wrangling and renaming columns
    logging.debug("Fact_immigration data wrangling")
    fact_immigration_new_column_names = ['uid', 'year', 'month', 'city_code', 'state_code', 'arrive_date', 'departure_date', 'mode', 'visa']
    fact_immigration = rename_columns(fact_immigration, dict(zip(fact_immigration_columns_to_select, fact_immigration_new_column_names)))

    fact_immigration = fact_immigration.withColumn('country', lit('United States'))

    fact_immigration = fact_immigration.withColumn('arrive_date', udf_datetime_from_sas(col('arrive_date')))
    fact_immigration = fact_immigration.withColumn('departure_date', udf_datetime_from_sas(col('departure_date')))

    # Data Quality Check - Check for and remove duplicates based on 'uid' column
    fact_immigration = check_and_remove_duplicates(fact_immigration, 'uid')
    fact_immigration = remove_rows_with_missing_values(fact_immigration, critical_columns=['uid', 'arrive_date', 'visa'])

    # Write fact_immigration table to parquet files partitioned by state and city
    logging.debug("Writing fact_immigration table to parquet files partitioned by state and city")
    fact_immigration.write.mode("overwrite").partitionBy('state_code') \
        .parquet(path=dst + 'fact_immigration')

    # Process dim_immigration_personal table
    logging.info("Processing dim_immigration_personal")
    logging.debug("Extracting columns to create dim_immigration_personal table")
    dim_personal_columns_to_select = ['cicid', 'i94cit', 'i94res', 'biryear', 'gender', 'insnum']
    dim_immigration_personal = df.select(dim_personal_columns_to_select).distinct() \
        .withColumn("immi_personal_id", monotonically_increasing_id())
    
    # Data Quality Check - Integrity Constraints (Primary Key)
    logging.info("Performing data quality check - Integrity Constraints (Primary Key)")
    assert dim_immigration_personal.count() == dim_immigration_personal.select("immi_personal_id").distinct().count(), \
        "Data quality check failed: Duplicate primary key values found in dim_immigration_personal."

    # Data wrangling and renaming columns
    logging.debug("Dim_immigration_personal data wrangling")
    dim_personal_new_column_names = ['uid', 'citizen_country', 'residence_country', 'birth_year', 'gender', 'ins_num']
    dim_immigration_personal = rename_columns(dim_immigration_personal, dict(zip(dim_personal_columns_to_select, dim_personal_new_column_names)))

    # Data Quality Check - Check for and remove duplicates based on 'uid' column
    dim_immigration_personal = check_and_remove_duplicates(dim_immigration_personal, 'uid')
    dim_immigration_personal = remove_rows_with_missing_values(dim_immigration_personal, critical_columns=['uid'])

    # Write dim_immigration_personal table to parquet files
    logging.debug("Writing dim_immigration_personal table to parquet files")
    dim_immigration_personal.write.mode("overwrite") \
        .parquet(path=dst + 'dim_immigration_personal')

    # Process dim_immigration_airline table
    logging.info("Processing dim_immigration_airline")
    logging.debug("Extracting columns to create dim_immigration_airline table")
    dim_airline_columns_to_select = ['cicid', 'airline', 'admnum', 'fltno', 'visatype']
    dim_immigration_airline = df.select(dim_airline_columns_to_select).distinct() \
        .withColumn("immi_airline_id", monotonically_increasing_id())

    # Data wrangling and renaming columns
    logging.debug("Dim_immigration_airline wrangling")
    dim_airline_new_column_names = ['uid', 'airline', 'admission_num', 'flight_number', 'visa_type']
    dim_immigration_airline = rename_columns(dim_immigration_airline, dict(zip(dim_airline_columns_to_select, dim_airline_new_column_names)))
    dim_immigration_airline = remove_rows_with_missing_values(dim_immigration_airline, critical_columns=['uid', 'admission_num'])

    # Data Quality Check - Check for and remove duplicates based on 'uid' column
    dim_immigration_airline = check_and_remove_duplicates(dim_immigration_airline, 'uid')

    # Write dim_immigration_airline table to parquet files
    logging.debug("Writing dim_immigration_airline table to parquet files")
    dim_immigration_airline.write.mode("overwrite") \
        .parquet(path=dst + 'dim_immigration_airline')

# Process label descriptions data
def process_label_descriptions(spark: SparkSession, src: str, dst: str) -> None:
    """
    Process label descriptions data and save it in parquet format.
    
    Args:
        spark (SparkSession): The Spark session instance.
        src (str): Source location of the data.
        dst (str): Destination location to save processed data.
    """
    logging.info("Processing label descriptions")
    logging.debug("Reading the description SAS file")
    label_description_filename = FileNames.immigration_sas_labels
    label_file = os.path.join(src + label_description_filename)
    with open(label_file) as f:
        contents = f.readlines()

    # Extract and process country codes
    logging.debug("Getting country codes")
    country_code = {}
    for countries in contents[10:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country

    # Create a DataFrame for country codes and write it to parquet files
    spark.createDataFrame(country_code.items(), ['code', 'country']) \
        .write.mode("overwrite") \
        .parquet(path=dst + 'country_code')

    # Extract and process city codes
    logging.debug("Getting city codes")
    city_code = {}
    for cities in contents[303:962]:
        pair = cities.split('=')
        code, city = pair[0].strip("\t").strip().strip("'"), \
                     pair[1].strip('\t').strip().strip("''")
        city_code[code] = city

    # Create a DataFrame for city codes and write it to parquet files
    spark.createDataFrame(city_code.items(), ['code', 'city']) \
        .write.mode("overwrite") \
        .parquet(path=dst + 'city_code')

    # Extract and process state codes
    logging.debug("Getting state codes")
    state_code = {}
    for states in contents[982:1036]:
        pair = states.split('=')
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        state_code[code] = state

    # Create a DataFrame for state codes and write it to parquet files
    spark.createDataFrame(state_code.items(), ['code', 'state']) \
        .write.mode("overwrite") \
        .parquet(path=dst + 'state_code')

# Process airport data
def process_airport_data(spark: SparkSession, src: str, dst: str) -> None:
    """
    Process airport data and save it in parquet format.
    
    Args:
        spark (SparkSession): The Spark session instance.
        src (str): Source location of the data.
        dst (str): Destination location to save processed data.
    """
    logging.info("Processing dim_airport")
    airport_data_file = FileNames.airport
    airport_data = os.path.join(src + airport_data_file)
    df = spark.read.csv(airport_data, header=True)

    # Select and rename columns
    logging.debug("Selecting and renaming columns")
    airport_columns_to_select = ['ident', 'type', 'name', 'elevation_ft', 'iso_country', 'iso_region', 'municipality', 'gps_code', 'local_code', 'coordinates']
    dim_airport = df.select(*airport_columns_to_select)

    airport_new_column_names = ['ident', 'type', 'airport_name', 'elevation_ft', 'iso_country', 'iso_region', 'municipality', 'gps_code', 'local_code', 'coordinates']
    dim_airport = rename_columns(dim_airport, dict(zip(airport_columns_to_select, airport_new_column_names)))

    # Data Quality Check - Check for and remove duplicates based on 'ident' column
    dim_airport = check_and_remove_duplicates(dim_airport, 'ident')
    dim_airport = remove_rows_with_missing_values(dim_airport, critical_columns=['ident', 'coordinates'])

    # Write dim_airport table to parquet files
    logging.debug("Writing dim_airport table to parquet files")
    dim_airport.write.mode("overwrite") \
        .parquet(path=dst + 'dim_airport')

# Process temperature data
def process_temperature_data(spark: SparkSession, src: str, dst: str) -> None:
    """
    Process temperature data and create dim_temperature table.

    Args:
        spark (SparkSession): SparkSession instance.
        src (str): Source directory for data.
        dst (str): Destination directory for storing processed data.

    Returns:
        None
    """
    logging.info("Start processing dim_temperature")
    logging.debug("Reading temperature data file")
    temperature_data_file = FileNames.temperature
    temperature_data = os.path.join(src + temperature_data_file)
    df = spark.read.csv(temperature_data, header=True)

    # Select US data
    logging.debug("Selecting US data")
    df = df.filter(df['Country'] == "United States")

    # Select required columns
    temperature_columns_to_select = ['dt', 'AverageTemperature', 'AverageTemperatureUncertainty', 'City', 'Country']
    dim_temperature = df.select(*temperature_columns_to_select).distinct()

    # Rename columns
    logging.debug("Renaming column names")
    temperature_new_column_names = ['date', 'avg_temperature', 'avg_temperature_uncertainty', 'city', 'country']
    dim_temperature = rename_columns(dim_temperature, dict(zip(temperature_columns_to_select, temperature_new_column_names)))

    # Data Quality Check - Check for and remove duplicates based on 'date' column
    dim_temperature = check_and_remove_duplicates(dim_temperature, 'date')
    dim_temperature = remove_rows_with_missing_values(dim_temperature, critical_columns=['date', 'city'])

    # Data wrangling
    dim_temperature = dim_temperature.withColumn('date', to_date(col('date')))
    dim_temperature = dim_temperature.withColumn('year', year(dim_temperature['date']))
    dim_temperature = dim_temperature.withColumn('month', month(dim_temperature['date']))

    # Write dim_temperature table to parquet files
    logging.debug("Writing dim_temperature table to parquet files")
    dim_temperature.write.mode("overwrite") \
        .parquet(path=dst + 'dim_temperature')

# Process demography data
def process_demography_data(spark: SparkSession, src: str, dst: str) -> None:
    """
    Process demography data and create dim_demography and dim_demography_stats tables.

    Args:
        spark (SparkSession): SparkSession instance.
        src (str): Source directory for data.
        dst (str): Destination directory for storing processed data.

    Returns:
        None
    """
    logging.info("Processing dim_demography")
    logging.debug("Reading demography data file")
    demography_file = FileNames.city_demographics
    demog_data = os.path.join(src + demography_file)
    df = spark.read.format('csv').options(header=True, delimiter=';').load(demog_data)

    # Select required columns and create dim_demography table
    logging.debug("Selecting required columns for dim_demography")
    demography_columns_to_select = ['City', 'State', 'Male Population', 'Female Population', 'Number of Veterans', 'Foreign-born', 'Race']
    dim_demography = df.select(demography_columns_to_select).distinct() \
        .withColumn("demog_pop_id", monotonically_increasing_id())

    # Rename columns
    logging.debug("Renaming column names for dim_demography")
    demography_new_column_names = [i.lower().replace(" ", "_") for i in demography_columns_to_select]
    dim_demography = rename_columns(dim_demography, dict(zip(demography_columns_to_select, demography_new_column_names)))

    dim_demography = remove_rows_with_missing_values(dim_demography, critical_columns=['city', 'state'])

    # Write dim_demography table to parquet files
    logging.debug("Writing dim_demography table to parquet files")
    dim_demography.write.mode("overwrite") \
        .parquet(path=dst + 'dim_demography')

    # Process dim_demography_stats table
    logging.info("Processing dim_demography_stats")
    demography_stats_columns_to_select = ['City', 'State', 'Median Age', 'Average Household Size']
    dim_demography_stats = df.select(demography_stats_columns_to_select) \
        .distinct() \
        .withColumn("demog_stat_id", monotonically_increasing_id())

    # Rename columns
    logging.debug("Renaming columns for dim_demography_stats")
    demography_stats_new_column_names = [i.lower().replace(" ", "_") for i in demography_stats_columns_to_select]
    dim_demography_stats = rename_columns(dim_demography_stats, dict(zip(demography_stats_columns_to_select, demography_stats_new_column_names)))
    dim_demography_stats = dim_demography_stats.withColumn('city', upper(col('city')))
    dim_demography_stats = dim_demography_stats.withColumn('state', upper(col('state')))

    dim_demography_stats = remove_rows_with_missing_values(dim_demography_stats, critical_columns=['city', 'state'])

    # Write dim_demography_stats table to parquet files
    logging.debug("Writing dim_demography_stats table to parquet files")
    dim_demography_stats.write.mode("overwrite") \
        .parquet(path=dst + 'dim_demography_stats')

# Main ETL process
def run_etl(local_mode=False) -> None:
    spark = create_spark_session()
    if local_mode:
        logging.info("Running in LOCAL mode")
        src = FileNames.local_srcS
        dst = FileNames.local_dst
    else:
        src = ReadConfigs.SOURCE_S3_BUCKET
        dst = ReadConfigs.DEST_S3_BUCKET

    logging.info("Processing initiated")
    process_immigration_data(spark, src, dst, use_sample_data=True)
    process_label_descriptions(spark, src, dst)
    process_temperature_data(spark, src, dst)
    process_demography_data(spark, src, dst)
    process_airport_data(spark, src, dst)
    logging.info("Processing completed")

# Entry point
if __name__ == "__main__":
    run_etl(local_mode=True)
    
