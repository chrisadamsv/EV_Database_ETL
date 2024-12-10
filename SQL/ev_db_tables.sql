CREATE TABLE cafv_eligibility (
    cafv_id VARCHAR(5) PRIMARY KEY
    CHECK (cafv_id LIKE 'cafv%'),
    cafv_eligibility VARCHAR
);


CREATE TABLE utility_companies (
    utility_company_id VARCHAR(5) PRIMARY KEY
    CHECK (utility_company_id LIKE 'uc%'),
    utility_company_name VARCHAR
);


CREATE TABLE county_income (
    fips_code VARCHAR(5) PRIMARY KEY,
    income_year VARCHAR(4) NOT NULL,
    percapita_income DECIMAL(10,2),
);    


CREATE TABLE location_info (
    postal_code VARCHAR(5) PRIMARY KEY,
    fips_code VARCHAR(5),
    FOREIGN KEY (fips_code) REFERENCES county_income (fips_code),
    county VARCHAR NOT NULL,
    city VARCHAR NOT NULL,
    state VARCHAR(2) NOT NULL,
    legislative_district VARCHAR,
    latitude DECIMAL(10,6) NOT NULL,
    longitude DECIMAL(10,6) NOT NULL,
    census_tract_2020 VARCHAR
);


CREATE TABLE vehicle_types (
    vehicle_type_id VARCHAR(6) PRIMARY KEY
    CHECK (vehicle_type_id LIKE 'vm%'),
    model_year VARCHAR(4) NOT NULL,
    make VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    ev_type VARCHAR
);


CREATE TABLE vehicles (
    dol_vehicle_id VARCHAR(9) PRIMARY KEY,
    vehicle_type_id VARCHAR(6) NOT NULL
    CHECK (vehicle_type_id LIKE 'vm%'),
    FOREIGN KEY (vehicle_type_id) REFERENCES vehicle_types (vehicle_type_id),
    postal_code VARCHAR(5) NOT NULL,
    FOREIGN KEY (postal_code) REFERENCES location_info (postal_code),
    fips_code VARCHAR(5),
    FOREIGN KEY (fips_code) REFERENCES county_income (fips_code),
    utility_company_id VARCHAR(5),
    CHECK (utility_company_id LIKE 'uc%'),
    FOREIGN KEY (utility_company_id) REFERENCES utility_companies (utility_company_id),
    cafv_id VARCHAR(5)
    CHECK (cafv_id LIKE 'cafv%'),
    FOREIGN KEY (cafv_id) REFERENCES cafv_eligibility (cafv_id),
    vin VARCHAR(10) NOT NULL,
    electric_range DECIMAL(10,1),
    base_msrp DECIMAL(10,2)
);