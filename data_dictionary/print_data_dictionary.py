table_columns = {
    'fact_immigration': {
        'Old Column Names': ['cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr', 'arrdate', 'depdate', 'i94mode', 'i94visa', 'country'],
        'New Column Names': ['uid', 'year', 'month', 'city_code', 'state_code', 'arrive_date', 'departure_date', 'mode', 'visa', 'country'],
        'Description': [
            'Unique identifier for each immigration record',
            '4-digit year of the immigration record',
            'Numeric month of the immigration record',
            'Port of entry for immigration',
            'State of arrival for immigration',
            'Arrival date in SAS format',
            'Departure date in SAS format',
            'Mode of transportation',
            'Visa type',
            'Country',
        ]
    },
    'dim_immigration_personal': {
        'Old Column Names': ['cicid', 'i94cit', 'i94res', 'biryear', 'gender', 'insnum'],
        'New Column Names': ['uid', 'citizen_country', 'residence_country', 'birth_year', 'gender', 'ins_num'],
        'Description': [
            'Unique identifier for each immigration personal record',
            'Citizen country code',
            'Residence country code',
            'Year of birth',
            'Gender of the immigrant',
            'INS number'
        ]
    },
    'dim_immigration_airline': {
        'Old Column Names': ['cicid', 'airline', 'admnum', 'fltno', 'visatype'],
        'New Column Names': ['uid', 'airline', 'admission_num', 'flight_number', 'visa_type'],
        'Description': [
            'Unique identifier for each immigration airline record',
            'Airline code',
            'Admission number',
            'Flight number',
            'Visa type'
        ]
    },
    'dim_temperature': {
        'Old Column Names': ['dt', 'AverageTemperature', 'AverageTemperatureUncertainty', 'City', 'Country'],
        'New Column Names': ['date', 'avg_temperature', 'avg_temperature_uncertainty', 'city', 'country'],
        'Description': [
            'Date of temperature measurement',
            'Average temperature',
            'Uncertainty in average temperature',
            'City where temperature was measured',
            'Country where temperature was measured'
        ]
    },
    'dim_demography': {
        'Old Column Names': ['City', 'State', 'Male Population', 'Female Population', 'Number of Veterans', 'Foreign-born', 'Race'],
        'New Column Names': ['city', 'state', 'male_population', 'female_population', 'num_veterans', 'foreign_born', 'race'],
        'Description': [
            'City name',
            'State name',
            'Male population count',
            'Female population count',
            'Number of veterans',
            'Count of foreign-born residents',
            'Race'
        ]
    },
    'dim_demography_stats': {
        'Old Column Names': ['City', 'State', 'Median Age', 'Average Household Size'],
        'New Column Names': ['city', 'state', 'median_age', 'avg_household_size'],
        'Description': [
            'City name',
            'State name',
            'Median age of residents',
            'Average household size'
        ]
    },
    'dim_airport': {
        'Old Column Names': ['ident', 'type', 'name', 'elevation_ft', 'iso_country', 'iso_region', 'municipality', 'gps_code', 'local_code', 'coordinates'],
        'New Column Names': ['ident', 'type', 'airport_name', 'elevation_ft', 'iso_country', 'iso_region', 'municipality', 'gps_code', 'local_code', 'coordinates'],
        'Description': [
            'Airport identifier',
            'Airport type',
            'Airport name',
            'Elevation in feet',
            'ISO country code',
            'ISO region code',
            'Municipality',
            'GPS code',
            'Local code',
            'Coordinates'
        ]
    }
}

# Create a beautiful text file
with open('data_dictionary/data_dictionary.txt', 'w') as file:
    file.write("/////////////////////\n")
    file.write("// Data Dictionary //\n")
    file.write("/////////////////////\n\n")

    for table, columns in table_columns.items():
        file.write("-------\n")
        file.write(f"{table}\n")
        file.write("-------\n")
        for old, new, desc in zip(columns['Old Column Names'], columns['New Column Names'], columns['Description']):
            file.write(f"- {old} -> {new}: {desc}\n")
        file.write("\n")