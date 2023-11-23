-- Create fact_immigration table
CREATE TABLE IF NOT EXISTS fact_immigration (
    uid INT PRIMARY KEY,
    year INT,
    month INT,
    city_code VARCHAR(255),
    state_code VARCHAR(255),
    arrive_date DATE,
    departure_date DATE,
    mode INT,
    visa INT,
    country VARCHAR(255)
);

-- Create dim_immigration_personal table
CREATE TABLE IF NOT EXISTS dim_immigration_personal (
    uid INT PRIMARY KEY,
    citizen_country INT,
    residence_country INT,
    birth_year INT,
    gender VARCHAR(255),
    ins_num VARCHAR(255),
    FOREIGN KEY (uid) REFERENCES fact_immigration(uid)
);

-- Create dim_immigration_airline table
CREATE TABLE IF NOT EXISTS dim_immigration_airline (
    uid INT PRIMARY KEY,
    airline VARCHAR(255),
    admission_num DOUBLE PRECISION,
    flight_number VARCHAR(255),
    visa_type VARCHAR(255),
    FOREIGN KEY (uid) REFERENCES fact_immigration(uid)
);

-- Create city_code table
CREATE TABLE IF NOT EXISTS city_code (
    code VARCHAR(255) PRIMARY KEY,
    city VARCHAR(255),
    FOREIGN KEY (code) REFERENCES fact_immigration(city_code)
);

-- Create us_states table
CREATE TABLE IF NOT EXISTS state_code (
    code TEXT(2) NOT NULL PRIMARY KEY,
    state VARCHAR(20) NOT NULL UNIQUE,
    FOREIGN KEY (code) REFERENCES fact_immigration(state_code)
);

-- Create dim_airport table
CREATE TABLE IF NOT EXISTS dim_airport (
    ident VARCHAR(255) PRIMARY KEY,
    type VARCHAR(255),
    airport_name VARCHAR(255),
    elevation_ft INT,
    iso_country VARCHAR(255),
    iso_region VARCHAR(255),
    municipality VARCHAR(255),
    gps_code VARCHAR(255),
    local_code VARCHAR(255),
    coordinates VARCHAR(255)
);

-- Create dim_temperature table
CREATE TABLE IF NOT EXISTS dim_temperature (
    date DATE PRIMARY KEY,
    avg_temperature DOUBLE PRECISION,
    avg_temperature_uncertainty DOUBLE PRECISION,
    city VARCHAR(255),
    country VARCHAR(255),
    year INT,
    month INT
);

-- Create dim_demography table
CREATE TABLE IF NOT EXISTS dim_demography (
    demog_pop_id INT PRIMARY KEY,
    city VARCHAR(255),
    state VARCHAR(255),
    male_population INT,
    female_population INT,
    number_of_veterans INT,
    foreign_born INT,
    race VARCHAR(255)
);

-- Create dim_demography_stats table
CREATE TABLE IF NOT EXISTS dim_demography_stats (
    demog_stat_id INT PRIMARY KEY,
    city VARCHAR(255),
    state VARCHAR(255),
    median_age DOUBLE PRECISION,
    average_household_size DOUBLE PRECISION,
    FOREIGN KEY (city) REFERENCES dim_demography(city),
    FOREIGN KEY (state) REFERENCES dim_demography(state)
);
