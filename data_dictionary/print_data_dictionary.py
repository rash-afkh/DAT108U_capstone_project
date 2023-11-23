table_columns = {
    'fact_immigration': {
        'Columns': [
            ('uid', 'Unique identifier for each immigration record', 'Integer', '12345'),
            ('year', '4-digit year of the immigration record', 'Integer', '2019'),
            ('month', 'Numeric month of the immigration record', 'Integer', '7'),
            ('city_code', 'Port of entry for immigration', 'String', 'NYC'),
            ('state_code', 'State of arrival for immigration', 'String', 'NY'),
            ('arrive_date', 'Arrival date in SAS format', 'Date', '2019-07-15'),
            ('departure_date', 'Departure date in SAS format', 'Date', '2019-07-25'),
            ('mode', 'Mode of transportation', 'Integer', '1'),
            ('visa', 'Visa type', 'Integer', '2'),
            ('country', 'Country', 'String', 'United States')
        ]
    },
    'dim_immigration_personal': {
        'Columns': [
            ('uid', 'Unique identifier for each immigration personal record', 'Integer', '67890'),
            ('citizen_country', 'Citizen country code', 'Integer', '123'),
            ('residence_country', 'Residence country code', 'Integer', '456'),
            ('birth_year', 'Year of birth', 'Integer', '1985'),
            ('gender', 'Gender of the immigrant', 'String', 'Male'),
            ('ins_num', 'INS number', 'String', '123456789')
        ]
    },
    'dim_immigration_airline': {
        'Columns': [
            ('uid', 'Unique identifier for each immigration airline record', 'Integer', '54321'),
            ('airline', 'Airline code', 'String', 'AA'),
            ('admission_num', 'Admission number', 'String', '987654321'),
            ('flight_number', 'Flight number', 'String', 'AA123'),
            ('visa_type', 'Visa type', 'String', 'B1')
        ]
    },
    'dim_temperature': {
        'Columns': [
            ('date', 'Date of temperature measurement', 'Date', '2019-07-15'),
            ('avg_temperature', 'Average temperature', 'Double', '78.5'),
            ('avg_temperature_uncertainty', 'Uncertainty in average temperature', 'Double', '0.5'),
            ('city', 'City where temperature was measured', 'String', 'New York'),
            ('country', 'Country where temperature was measured', 'String', 'United States')
        ]
    },
    'dim_demography': {
        'Columns': [
            ('city', 'City name', 'String', 'New York'),
            ('state', 'State name', 'String', 'New York'),
            ('male_population', 'Male population count', 'Integer', '50000'),
            ('female_population', 'Female population count', 'Integer', '55000'),
            ('num_veterans', 'Number of veterans', 'Integer', '7500'),
            ('foreign_born', 'Count of foreign-born residents', 'Integer', '2000'),
            ('race', 'Race', 'String', 'White')
        ]
    },
    'dim_demography_stats': {
        'Columns': [
            ('city', 'City name', 'String', 'New York'),
            ('state', 'State name', 'String', 'New York'),
            ('median_age', 'Median age of residents', 'Double', '35.5'),
            ('avg_household_size', 'Average household size', 'Double', '2.5')
        ]
    },
    'dim_airport': {
        'Columns': [
            ('ident', 'Airport identifier', 'String', 'JFK'),
            ('type', 'Airport type', 'String', 'Large'),
            ('airport_name', 'Airport name', 'String', 'John F. Kennedy International Airport'),
            ('elevation_ft', 'Elevation in feet', 'Integer', '10'),
            ('iso_country', 'ISO country code', 'String', 'US'),
            ('iso_region', 'ISO region code', 'String', 'US-NY'),
            ('municipality', 'Municipality', 'String', 'New York'),
            ('gps_code', 'GPS code', 'String', 'JFK'),
            ('local_code', 'Local code', 'String', 'JFK'),
            ('coordinates', 'Coordinates', 'String', '40.6413° N, 73.7781° W')
        ]
    }
}

# Create a beautiful Markdown file
with open('data_dictionary/data_dictionary.md', 'w') as file:
    file.write("# Data Dictionary\n\n")

    for table, columns in table_columns.items():
        file.write(f"## {table}\n")
        file.write("| Column | Description | Data Type | Example |\n")
        file.write("|--------|-------------|-----------|---------|\n")
        for col, desc, data_type, example in columns['Columns']:
            file.write(f"| {col} | {desc} | {data_type} | {example} |\n")
        file.write("\n")