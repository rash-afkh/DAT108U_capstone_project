# Capstone Project: Data Engineering

## Project Overview

### US Immigration Data Analysis Project

#### Overview

This project is aimed at building a robust ETL (Extract, Transform, Load) pipeline that aggregates data from seven distinct sources and prepares it for storage within a data warehouse. The primary objective is to facilitate data analysis of the US immigration phenomenon, leveraging Business Intelligence applications. With this data warehouse, we can gain valuable insights into various aspects, including:

-    Investigating potential correlations between global warming and immigrant origins.
-    Identifying trends in immigration from specific states, whether increasing or decreasing.
-    Determining the primary destination states for immigrants.
-    Analyzing the age distribution of immigrants, among other insights.

#### Project Background

This repository represents the culmination of the Data Engineering Nanodegree program on Udacity. The code has undergone rigorous testing in the Udacity Project Workspace environment to ensure its reliability and functionality.

## Project Structure

The project directory is organized as follows:

- **data**: This folder contains the data files required for the project.
- **data_dictionary**: This folder contains a data structural file that provides information about the data schema.
- **logs**: This folder contains log files generated during the ETL process.
- **tables**: This folder is used to store the final Parquet tables generated as part of the ETL process.

The main project files are:

- **etl.py**: This Python script contains the ETL logic for processing and transforming the data.
- **tools.py**: This Python script contains utility functions used in the ETL process.
- **Capstone_Project.ipynb**: This Jupyter Notebook serves as the main project workspace and contains code for running the ETL process.

# ETL (Extract, Transform, Load) Process

The ETL process is a crucial part of data integration, where data is extracted from various sources, transformed into the desired format, and loaded into a data warehouse or storage system for further analysis. In this project, the `etl.py` script performs the ETL process for the immigration and related datasets.

## etl.py

The `etl.py` script is responsible for extracting, transforming, and loading the data from various sources into structured tables. Below is an overview of what the `etl.py` script does:

### Configuration Setup

The script starts by setting up configurations using the `configparser` library, which reads parameters from the `config.cfg` file. These configurations include AWS access keys, S3 bucket paths, and more.

### Spark Session Creation

A Spark session is created using PySpark. Spark is a powerful framework for distributed data processing, making it suitable for handling large datasets.

### Data Extraction

1. **Immigration Data**: The script extracts immigration data from either sample CSV files or SAS7BDAT format files, depending on whether the `use_sample_data` flag is set to `True`. It uses the `pyspark` library to read the data.

2. **Label Descriptions**: The script reads label descriptions, such as country codes, city codes, and state codes, from the SAS file. These descriptions are then used to enrich the data.

3. **Airport Data**: The script extracts airport data from a CSV file.

4. **Temperature Data**: The script extracts temperature data for the United States from a CSV file.

5. **Demography Data**: The script extracts city demography data from a CSV file.

### Data Transformation

1. **Fact and Dimension Tables**: The extracted data is transformed into fact and dimension tables using PySpark DataFrame operations. Columns are renamed and cleaned as necessary, and data types are adjusted.

2. **Data Quality Checks**: Data quality checks are performed to ensure the integrity of the data, including checking for duplicate primary key values.

3. **Data Wrangling**: Data wrangling operations, such as date conversions, are applied to prepare the data for loading.

### Data Loading

1. **Parquet Files**: The transformed data is written to Parquet files. Fact tables are partitioned by state code for optimized querying.

2. **Country, City, and State Codes**: The script creates additional tables for country, city, and state codes, which can be used for reference.

### Running the ETL Process

The script can be executed by running `etl.py`. By default, it runs in local mode, but you can configure it to run in a distributed cluster environment by adjusting the configuration settings.
You can specify whether to use sample data or full data by setting the `use_sample_data` flag.

```bash
python etl.py
```

To run the ETL process on AWS, make sure to configure your AWS credentials in the `config.cfg` file.

## Logging

Logging is implemented to capture information about the ETL process. Log files are stored in the logs folder, and each log file is timestamped.
Data Dictionary



## Data Dictionary
A data structural file (data_dictionary) is provided in the data_dictionary folder, which provides information about the schema of the final Parquet tables.
Conclusion

Data dictionary format is:
`old_column_name` -> `new_column_name`: `description`

## fact_immigration

- `cicid` -> `uid`: Unique identifier for each immigration record
- `i94yr` -> `year`: 4-digit year of the immigration record
- `i94mon` -> `month`: Numeric month of the immigration record
- `i94port` -> `city_code`: Port of entry for immigration
- `i94addr` -> `state_code`: State of arrival for immigration
- `arrdate` -> `arrive_date`: Arrival date in SAS format
- `depdate` -> `departure_date`: Departure date in SAS format
- `i94mode` -> `mode`: Mode of transportation
- `i94visa` -> `visa`: Visa type

## dim_immigration_personal

- `cicid` -> `uid`: Unique identifier for each immigration personal record
- `i94cit` -> `citizen_country`: Citizen country code
- `i94res` -> `residence_country`: Residence country code
- `biryear` -> `birth_year`: Year of birth
- `gender` -> `gender`: Gender of the immigrant
- `insnum` -> `ins_num`: INS number

## dim_immigration_airline

- `cicid` -> `uid`: Unique identifier for each immigration airline record
- `airline` -> `airline`: Airline code
- `admnum` -> `admission_num`: Admission number
- `fltno` -> `flight_number`: Flight number
- `visatype` -> `visa_type`: Visa type

## dim_temperature

- `dt` -> `date`: Date of temperature measurement
- `AverageTemperature` -> `avg_temperature`: Average temperature
- `AverageTemperatureUncertainty` -> `avg_temperature_uncertainty`: Uncertainty in average temperature
- `City` -> `city`: City where temperature was measured
- `Country` -> `country`: Country where temperature was measured

## dim_demography

- `City` -> `city`: City name
- `State` -> `state`: State name
- `Male Population` -> `male_population`: Male population count
- `Female Population` -> `female_population`: Female population count
- `Number of Veterans` -> `num_veterans`: Number of veterans
- `Foreign-born` -> `foreign_born`: Count of foreign-born residents
- `Race` -> `race`: Race

## dim_demography_stats

- `City` -> `city`: City name
- `State` -> `state`: State name
- `Median Age` -> `median_age`: Median age of residents
- `Average Household Size` -> `avg_household_size`: Average household size

## dim_airport

- `ident` -> `ident`: Airport identifier
- `type` -> `type`: Airport type
- `name` -> `airport_name`: Airport name
- `elevation_ft` -> `elevation_ft`: Elevation in feet
- `iso_country` -> `iso_country`: ISO country code
- `iso_region` -> `iso_region`: ISO region code
- `municipality` -> `municipality`: Municipality
- `gps_code` -> `gps_code`: GPS code
- `local_code` -> `local_code`: Local code
- `coordinates` -> `coordinates`: Coordinates

This Capstone project demonstrates the end-to-end process of designing and implementing a data warehouse using Apache Spark for data extraction, transformation, and loading. The resulting data warehouse can be used for analytical purposes and provides valuable insights into immigration, temperature, demography, and airport data.

For more details and code implementation, please refer to the Jupyter Notebook (Capstone_Project.ipynb) and the Python scripts.
