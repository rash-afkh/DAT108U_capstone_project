import configparser
import os
from tools import setup_logger, rename_columns, FileNames, udf_datetime_from_sas
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month, upper, to_date, udf
from pyspark.sql.functions import monotonically_increasing_id


setup_logger()

class ReadConfigs():
    config = configparser.ConfigParser()
    config.read('config.cfg', encoding='utf-8-sig')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
    DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']


# Create spark session
def create_spark_session() -> SparkSession:
    logging.info("Creating spark session")
    spark = SparkSession.builder \
        .config("spark.jars.packages",
                "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport().getOrCreate()
    return spark


def process_immigration_data(spark: SparkSession, src: str, dst: str, use_sample_data=False) -> None:

    logging.info("processing immigration data")
    if use_sample_data:
        immigration_data_path = os.path.join(src + FileNames.immigration_sample)
        df = spark.read.option('header',True).csv(immigration_data_path)
    else:
        immigration_data_path = os.path.join(src + FileNames.immigration_sas)
        df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data_path)

    logging.info("processing fact_immigration")
    logging.debug("extracting columns to create fact_immigration table")
    fact_immigration_columns_to_select = ['cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', 'arrdate', 'depdate', 'i94mode', 'i94visa']
    fact_immigration = df.select(fact_immigration_columns_to_select).distinct() \
        .withColumn("immigration_id", monotonically_increasing_id())

    # Data Quality Check - Integrity Constraints (Primary Key)
    logging.info("Performing data quality check - Integrity Constraints (Primary Key)")
    assert fact_immigration.count() == fact_immigration.select("immigration_id").distinct().count(), \
        "Data quality check failed: Duplicate primary key values found in fact_immigration."

    logging.debug("fact_mmigration data wrangling")
    fact_immigration_new_column_names = ['uid', 'year', 'month', 'city_code', 'state_code', 'arrive_date', 'departure_date', 'mode', 'visa']
    fact_immigration = rename_columns(fact_immigration, dict(zip(fact_immigration_columns_to_select, fact_immigration_new_column_names)))

    fact_immigration = fact_immigration.withColumn('country', lit('United States'))

    fact_immigration = fact_immigration.withColumn('arrive_date', udf_datetime_from_sas(col('arrive_date')))
    fact_immigration = fact_immigration.withColumn('departure_date', udf_datetime_from_sas(col('departure_date')))


    logging.debug("writing fact_immigration table to parquet files partitioned by state and city")
    fact_immigration.write.mode("overwrite").partitionBy('state_code') \
        .parquet(path=dst + 'fact_immigration')

    logging.info("processing dim_immigration_personal")
    logging.debug("extracting columns to create dim_immigration_personal table")
    dim_personal_columns_to_select = ['cicid', 'i94cit', 'i94res', 'biryear', 'gender', 'insnum']
    dim_immigration_personal = df.select(dim_personal_columns_to_select).distinct() \
        .withColumn("immi_personal_id", monotonically_increasing_id())
    
    # Data Quality Check - Integrity Constraints (Primary Key)
    logging.info("Performing data quality check - Integrity Constraints (Primary Key)")
    assert dim_immigration_personal.count() == dim_immigration_personal.select("immi_personal_id").distinct().count(), \
        "Data quality check failed: Duplicate primary key values found in dim_immigration_personal."

    logging.debug("dim_immigration_personal data wrangling")
    dim_personal_new_column_names = ['uid', 'citizen_country', 'residence_country', 'birth_year', 'gender', 'ins_num']
    dim_immigration_personal = rename_columns(dim_immigration_personal, dict(zip(dim_personal_columns_to_select, dim_personal_new_column_names)))

    logging.debug("writing dim_immigration_personal table to parquet files")
    dim_immigration_personal.write.mode("overwrite") \
        .parquet(path=dst + 'dim_immigration_personal')

    logging.info("processing dim_immigration_airline")
    logging.debug("extracting columns to create dim_immigration_airline table")
    dim_airline_columns_to_select = ['cicid', 'airline', 'admnum', 'fltno', 'visatype']
    dim_immigration_airline = df.select(dim_airline_columns_to_select).distinct() \
        .withColumn("immi_airline_id", monotonically_increasing_id())

    logging.debug("dim_immigration_airline wrangling")
    dim_airline_new_column_names = ['uid', 'airline', 'admission_num', 'flight_number', 'visa_type']
    dim_immigration_airline = rename_columns(dim_immigration_airline, dict(zip(dim_airline_columns_to_select, dim_airline_new_column_names)))

    logging.debug("writing dim_immigration_airline table to parquet files")
    dim_immigration_airline.write.mode("overwrite") \
        .parquet(path=dst + 'dim_immigration_airline')


def process_label_descriptions(spark: SparkSession, src: str, dst: str) -> None:

    logging.info("processing label descriptions")
    logging.debug("Reading the description SAS file")
    label_description_filename = FileNames.immigration_sas_labels
    label_file = os.path.join(src + label_description_filename)
    with open(label_file) as f:
        contents = f.readlines()

    logging.debug("getting country codes")
    country_code = {}
    for countries in contents[10:298]:
        pair = countries.split('=')
        code, country = pair[0].strip(), pair[1].strip().strip("'")
        country_code[code] = country
    spark.createDataFrame(country_code.items(), ['code', 'country']) \
        .write.mode("overwrite") \
        .parquet(path=dst + 'country_code')

    logging.debug("getting city codes")
    city_code = {}
    for cities in contents[303:962]:
        pair = cities.split('=')
        code, city = pair[0].strip("\t").strip().strip("'"), \
                     pair[1].strip('\t').strip().strip("''")
        city_code[code] = city
    spark.createDataFrame(city_code.items(), ['code', 'city']) \
        .write.mode("overwrite") \
        .parquet(path=dst + 'city_code')

    logging.debug("getting state codes")
    state_code = {}
    for states in contents[982:1036]:
        pair = states.split('=')
        code, state = pair[0].strip('\t').strip("'"), pair[1].strip().strip("'")
        state_code[code] = state
    spark.createDataFrame(state_code.items(), ['code', 'state']) \
        .write.mode("overwrite") \
        .parquet(path=dst + 'state_code')

def process_airport_data(spark: SparkSession, src: str, dst: str) -> None:
    logging.info("processing dim_airport")
    airport_data_file = FileNames.airport
    airport_data = os.path.join(src + airport_data_file)
    df = spark.read.csv(airport_data, header=True)

    logging.debug("selecting and rename columns")
    airport_columns_to_select = ['ident', 'type', 'name', 'elevation_ft', 'iso_country', 'iso_region', 'municipality', 'gps_code', 'local_code', 'coordinates']
    dim_airport = df.select(*airport_columns_to_select)

    airport_new_column_names = ['ident', 'type', 'airport_name', 'elevation_ft', 'iso_country', 'iso_region', 'municipality', 'gps_code', 'local_code', 'coordinates']
    dim_airport = rename_columns(dim_airport, dict(zip(airport_columns_to_select, airport_new_column_names)))

    logging.debug("writing dim_airport table to parquet files")
    dim_airport.write.mode("overwrite") \
        .parquet(path=dst + 'dim_airport')
    
def process_temperature_data(spark: SparkSession, src: str, dst: str) -> None:

    logging.info("Start processing dim_temperature")
    logging.debug("reading temperature data file")
    temperature_data_file = FileNames.temperature
    temperature_data = os.path.join(src + temperature_data_file)
    df = spark.read.csv(temperature_data, header=True)

    logging.debug("selecting US data")
    df = df.filter(df['Country'] == "United States")

    logging.debug("selecting required columns")
    temperature_columns_to_select = ['dt', 'AverageTemperature', 'AverageTemperatureUncertainty', 'City', 'Country']
    dim_temperature = df.select(*temperature_columns_to_select).distinct()

    logging.debug("renaming column names")
    temperature_new_column_names = ['date', 'avg_temperature', 'avg_temperature_uncertainty', 'city', 'country']
    dim_temperature = rename_columns(dim_temperature, dict(zip(temperature_columns_to_select, temperature_new_column_names)))

    logging.debug("dim_temperature data wrangling")

    dim_temperature = dim_temperature.withColumn('date', to_date(col('date')))
    dim_temperature = dim_temperature.withColumn('year', year(dim_temperature['date']))
    dim_temperature = dim_temperature.withColumn('month', month(dim_temperature['date']))

    logging.debug("writing dim_temperature table to parquet files")
    dim_temperature.write.mode("overwrite") \
        .parquet(path=dst + 'dim_temperature')


def process_demography_data(spark: SparkSession, src: str, dst: str) -> None:

    logging.info("processing dim_demography")
    logging.debug("reading demography data file")
    demography_file = FileNames.city_demographics
    demog_data = os.path.join(src + demography_file)
    df = spark.read.format('csv').options(header=True, delimiter=';').load(demog_data)

    logging.debug("selecting required columns")
    demography_columns_to_select = ['City', 'State', 'Male Population', 'Female Population', 'Number of Veterans', 'Foreign-born', 'Race']
    dim_demography = df.select(demography_columns_to_select).distinct() \
        .withColumn("demog_pop_id", monotonically_increasing_id())

    logging.debug("renaming column names")
    demography_new_column_names = [i.lower().replace(" ", "_") for i in demography_columns_to_select]
    dim_demography = rename_columns(dim_demography, dict(zip(demography_columns_to_select, demography_new_column_names)))

    logging.debug("writing dim_demography table to parquet files")
    dim_demography.write.mode("overwrite") \
        .parquet(path=dst + 'dim_demography')

    logging.info("processing dim_demography_stats")
    demography_stats_columns_to_select = ['City', 'State', 'Median Age', 'Average Household Size']
    dim_demography_stats = df.select(demography_stats_columns_to_select) \
        .distinct() \
        .withColumn("demog_stat_id", monotonically_increasing_id())

    logging.debug("renaming columns")
    demography_stats_new_column_names = [i.lower().replace(" ", "_") for i in demography_stats_columns_to_select]
    dim_demography_stats = rename_columns(dim_demography_stats, dict(zip(demography_stats_columns_to_select, demography_stats_new_column_names)))
    dim_demography_stats = dim_demography_stats.withColumn('city', upper(col('city')))
    dim_demography_stats = dim_demography_stats.withColumn('state', upper(col('state')))

    logging.debug("writing dim_demography_stats table to parquet files")
    dim_demography_stats.write.mode("overwrite") \
        .parquet(path=dst + 'dim_demography_stats')


def run_etl(local_mode=False) -> None:
    spark = create_spark_session()
    if local_mode:
        logging.info("Running in LOCAL mode")
        src = FileNames.local_src
        dst = FileNames.local_dst
    else:
        src = ReadConfigs.SOURCE_S3_BUCKET
        dst = ReadConfigs.DEST_S3_BUCKET

    logging.info("processing initiated")
    process_immigration_data(spark, src, dst, use_sample_data=True)
    process_label_descriptions(spark, src, dst)
    process_temperature_data(spark, src, dst)
    process_demography_data(spark, src, dst)
    process_airport_data(spark, src, dst)
    logging.info("processing completed")


if __name__ == "__main__":
    run_etl(local_mode=True)