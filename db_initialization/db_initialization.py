import psycopg2
import pandas as pd
import requests
import json
from datetime import datetime
# Import postgres server info key
from postgres_info import user, password
from api_keys import bea_key

#############################################################
# Import income data by county from BEA API
#############################################################

#Create variables for the API call to get Income data by county
base_url = 'https://apps.bea.gov/api/data'
tablename = 'CAINC1' #per capita income by county
linecode = '1'
# Get the curent year 
previous_year = datetime.now().year-2

print('Pulling data from API...')
#Create URL to Bureau of Economic Analysis for per capita income by county by year
income_url = f'{base_url}?&UserID={bea_key}&method=GetData&datasetname=Regional&TableName={tablename}&LineCode={linecode}&Year={previous_year}&GeoFips=COUNTY&ResultFormat=json'
#Pull the response into the notebook
income_json = requests.get(income_url).json()
print('Data extracted successfully!')

#Display the result as a dataframe with 'json_normalize()' using only relevant columns 
json_df = pd.json_normalize(income_json['BEAAPI']['Results']['Data'])
income_df = json_df[['GeoFips','GeoName','TimePeriod','DataValue']]
#Format columns to match 'hpi_df' dataframe so that it can be merged later.
income_df = income_df.rename(columns={'GeoFips':'FIPS code','TimePeriod':'Year','GeoName':'County & State','DataValue':'PerCapita Income'})

#Format 'year' and 'FIPS code' as string add forward 0 back to the 'FIPS code'
income_df['Year'] = income_df['Year'].astype(str)
income_df['FIPS code'] = income_df['FIPS code'].astype(str)
income_df['FIPS code'] = income_df['FIPS code'].apply(lambda x: x.zfill(5))
# Rename 'Year' column to 'Income Year'
income_df = income_df.rename(columns={'Year':'Income Year'})
# Split County and State at the comma
income_df['County'] = income_df['County & State'].str.extract(r'^([^,]+)')
income_df['State'] = income_df['County & State'].str.extract(r',([^,]+)$')
# Remove '*' from the end of the state value if it exists
income_df['State'] = income_df['State'].str.rstrip('*')
income_df['State'] = income_df['State'].str.lstrip()
# Drop old 'County & State' column
income_df = income_df.drop(columns=['County & State'])
# Convert PerCapita Income to Float
income_df['PerCapita Income'] = income_df['PerCapita Income'].astype(int)
income_df['PerCapita Income'] = (income_df['PerCapita Income']/100).astype(float)
# Save new csv 
income_df.to_csv('Output_CSVs/income_data.csv',index=False)

#############################################################
# Import EV CSV data into python
#############################################################

# Upload EV csv to dataframe
main_df = pd.read_csv("Resources/Electric_Vehicle_Population_Data.csv")

# Drop rows where the postal code is NaN
main_df = main_df.dropna(subset=['Postal Code'])

# Convert fields to strings and remove decimals
main_df[['DOL Vehicle ID','Model Year']] = main_df[['DOL Vehicle ID','Model Year']].astype(str)
main_df['2020 Census Tract'] = main_df['2020 Census Tract'].astype(int).astype(str)
main_df['Postal Code'] = main_df['Postal Code'].astype(int).astype(str)
# Convert Legislative District field to string and then remove decimal
main_df['Legislative District'] = main_df['Legislative District'].astype(str).str.replace(r'\.0$', '', regex=True)
# Replace 'nan' in Legislative District with empty value
main_df['Legislative District'] = main_df['Legislative District'].str.replace('nan','')

# Extract lat and long from vehicle location column
main_df['Longitude'] = main_df['Vehicle Location'].str.extract(r'(-\d+\.\d+)', expand=False).astype(float)
main_df['Latitude'] = main_df['Vehicle Location'].str.extract(r' (\d+\.\d+)', expand=False).astype(float)

# Drop Vehicle Location column
main_df = main_df.drop('Vehicle Location', axis=1)

# Merge income data frome above dataframe 'income_df'
new_main_df = pd.merge(main_df, income_df, on=['County','State'])

#############################################################
# Create location Dataframe
#############################################################

# Create dataframe with location based columns
vehicle_location_df = new_main_df[['Postal Code','FIPS code','County','City','State','Legislative District','Latitude','Longitude','2020 Census Tract']]
# Get unique values by ridding postal code duplicates
location_df = vehicle_location_df.drop_duplicates(subset='Postal Code').sort_values(by=['Postal Code'], ascending=False)
# Save as csv
location_df.to_csv("Output_CSVs/location.csv", index=False)

#############################################################
# Create vehicle info Dataframe
#############################################################

# Create dataframe with vehicle info columns
vehicle_types_df = main_df[['Model Year','Make','Model','Electric Vehicle Type']].sort_values(by=['Model Year','Make','Model'], ascending=True)
vehicle_types_df = vehicle_types_df.drop_duplicates()
# Save as csv and reupload to reset index
vehicle_types_df.to_csv("Output_CSVs/vehicle_types.csv", index=False)
vehicle_types_df = pd.read_csv("Output_CSVs/vehicle_types.csv")

# Re-convert Model Year column to a string
vehicle_types_df['Model Year'] = vehicle_types_df['Model Year'].astype(str)

# Create a new column to hold new vehicle id. Use the columns index and add one digit
vehicle_types_df['Vehicle Type ID'] = vehicle_types_df.index+1
# Add 'vm' to the front of every ID
vehicle_types_df['Vehicle Type ID'] = vehicle_types_df['Vehicle Type ID'].astype(str).apply(lambda x: x.zfill(4))
vehicle_types_df['Vehicle Type ID'] = vehicle_types_df['Vehicle Type ID'].apply(lambda x: f'vm{x}')
# Move ID column to the front of dataframe
vehicle_types_df = vehicle_types_df[['Vehicle Type ID'] + [col for col in vehicle_types_df.columns if col != 'Vehicle Type ID']]

# Resave as CSV
vehicle_types_df.to_csv("Output_CSVs/vehicle_types.csv", index=False)

#############################################################
# Create CAFV Eligibility Dataframe
#############################################################

# Create CAFV Eligibility table
cafv_df = main_df[['Clean Alternative Fuel Vehicle (CAFV) Eligibility']].drop_duplicates()
# Save as CSV to reset index
cafv_df.to_csv("Output_CSVs/csfv_eligibility.csv", index=False)
# Reopen CSV
cafv_df = pd.read_csv("Output_CSVs/csfv_eligibility.csv")
# Create new column with index for ID
cafv_df['CAFV Eligibility ID'] = cafv_df.index+1
# Add 'cafv' to the front of the id numbers
cafv_df['CAFV Eligibility ID'] = cafv_df['CAFV Eligibility ID'].apply(lambda x: f'cafv{x}')
# Move ID First
cafv_df = cafv_df[['CAFV Eligibility ID','Clean Alternative Fuel Vehicle (CAFV) Eligibility']]
# Resave as CSV
cafv_df.to_csv("Output_CSVs/csfv_eligibility.csv", index=False)

#############################################################
# Create Utility Companies Dataframe
#############################################################

# Create a new dataframe with the utility company name
utilities_df = main_df[['Electric Utility']].drop_duplicates()
# Save as CSV to reset index
utilities_df.to_csv("Output_CSVs/utilities.csv", index=False)
# Reopen CSV
utilities_df = pd.read_csv("Output_CSVs/utilities.csv")
# Create new column with index for ID
utilities_df['Electric Company ID'] = utilities_df.index+1
# Add 'cafv' to the front of the id numbers
utilities_df['Electric Company ID'] = utilities_df['Electric Company ID'].astype(str).apply(lambda x: x.zfill(3))
utilities_df['Electric Company ID'] = utilities_df['Electric Company ID'].apply(lambda x: f'uc{x}')
# Move ID First
utilities_df = utilities_df[['Electric Company ID','Electric Utility']]
# Resave as CSV
utilities_df.to_csv("Output_CSVs/utilities.csv", index=False)

#############################################################
# Create main vehicle Dataframe
#############################################################

# Add Vehicle Type ID to main dataframe 
vehicle_df = pd.merge(new_main_df,vehicle_types_df, on=['Model Year','Make','Model','Electric Vehicle Type'])
vehicle_df = pd.merge(vehicle_df,cafv_df, on='Clean Alternative Fuel Vehicle (CAFV) Eligibility')
vehicle_df = pd.merge(vehicle_df,utilities_df, on='Electric Utility')
vehicle_df = vehicle_df[['DOL Vehicle ID','Vehicle Type ID','Postal Code','FIPS code','Electric Company ID','CAFV Eligibility ID','VIN (1-10)','Electric Range','Base MSRP']]
# Save as csv
vehicle_df.to_csv("Output_CSVs/vehicle.csv", index=False)

#############################################################
# Convert above dataframes to lists
#############################################################

cafv_list = cafv_df.values.tolist()
utilities_list = utilities_df.values.tolist()
income_list = income_df.values.tolist()
location_list = location_df.values.tolist()
vehicle_types_list = vehicle_types_df.values.tolist()
vehicles_list = vehicle_df.values.tolist()

#############################################################
# Connect to PostGRES to create new ev database
#############################################################

# Connect to PostGres to add database
try:
    # Connect to the database
    conn = psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        user=user,
        password=password,
        database="postgres"
    )
    conn.autocommit = True
except psycopg2.Error as e:
    print("Error connecting to the database:")
    print(e)
else:
    print("Connection established successfully")

# Create cursor
cur = conn.cursor()

# Query to create database
sql = '''CREATE DATABASE ev_db;'''

# Create database and commit
try:
    cur.execute(sql)
    print("Database successfully created!")
except:
    print("Database already exists! Execute command aborted...")


# Close connection
cur.close()
conn.close()

#############################################################
# Create new tables and add data in PostGRES
#############################################################

try:
    # Connect to the database
    conn = psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        user=user,
        password=password,
        database="ev_db"
    )
except psycopg2.Error as e:
    print("Error connecting to the database:")
    print(e)
else:
    print("Connection re-established successfully")

# Create cursor
cur = conn.cursor() 

# SQL query to drop table if already exists
drop_cafv_table_query = '''DROP TABLE IF EXISTS cafv_eligibility CASCADE;'''
drop_utilities_query = '''DROP TABLE IF EXISTS utility_companies CASCADE'''
drop_income_table_query = '''DROP TABLE IF EXISTS county_income CASCADE'''
drop_location_info_query = '''DROP TABLE IF EXISTS location_info CASCADE;'''
drop_vehicle_types_query = '''DROP TABLE IF EXISTS vehicle_types CASCADE;'''
drop_vehicles_query = '''DROP TABLE IF EXISTS vehicles CASCADE;'''

# SQL query to create a table
create_cafv_table_query = '''
    CREATE TABLE cafv_eligibility (
        cafv_id VARCHAR(5) PRIMARY KEY
        CHECK (cafv_id LIKE 'cafv%'),
        cafv_eligibility VARCHAR
    );
    '''

create_utilities_table_query = '''
    CREATE TABLE utility_companies (
        utility_company_id VARCHAR(5) PRIMARY KEY
        CHECK (utility_company_id LIKE 'uc%'),
        utility_company_name VARCHAR
    );
    '''

create_income_table_query = '''
    CREATE TABLE county_income (
        fips_code VARCHAR(5) PRIMARY KEY,
        income_year VARCHAR(4) NOT NULL,
        percapita_income DECIMAL(10,2),
        county VARCHAR NOT NULL,
        state VARCHAR(2)
    );    
    '''

create_location_info_query = """
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
    """

create_vehicle_types_query = """
    CREATE TABLE vehicle_types (
        vehicle_type_id VARCHAR(6) PRIMARY KEY
        CHECK (vehicle_type_id LIKE 'vm%'),
        model_year VARCHAR(4) NOT NULL,
        make VARCHAR NOT NULL,
        model VARCHAR NOT NULL,
        ev_type VARCHAR
    );
    """

create_vehicles_query = """
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
    """

# Execute Drop table query 
cur.execute(drop_cafv_table_query)
# Execute the CREATE TABLE query
cur.execute(create_cafv_table_query)
print("CAFV table created or recreated successfully!")

# Execute Drop table query 
cur.execute(drop_utilities_query)
# Execute the CREATE TABLE query
cur.execute(create_utilities_table_query)
print("Utility Company table created or recreated successfully!")

# Execute Drop table query 
cur.execute(drop_income_table_query)
# Execute the CREATE TABLE query
cur.execute(create_income_table_query)
print("County Income table created or recreated successfully!")

# Create database and commit
cur.execute(drop_location_info_query)
# Execute the CREATE TABLE query
cur.execute(create_location_info_query)
print("Location info table created or recreated successfully!")

# Create database and commit
cur.execute(drop_vehicle_types_query)
# Execute the CREATE TABLE query
cur.execute(create_vehicle_types_query)
print("Vehicle types table created or recreated successfully!")

# Create database and commit
cur.execute(drop_vehicles_query)
# Execute the CREATE TABLE query
cur.execute(create_vehicles_query)
print("Vehicles table created or recreated successfully!")

# Use a for loop to insert each row of data into location table
for row in cafv_list:
    sql = "INSERT INTO cafv_eligibility (cafv_id,cafv_eligibility) VALUES (%s, %s)"
    cur.execute(sql, row)
 
print('CAFV Elgibility Data added successfully!')

# Use a for loop to insert each row of data into location table
for row in utilities_list:
    sql = "INSERT INTO utility_companies (utility_company_id,utility_company_name) VALUES (%s, %s)"
    cur.execute(sql, row)
 
print('Utility Company Data added successfully!')

# Use a for loop to insert each row of data into location table
for row in income_list:
    sql = "INSERT INTO county_income (fips_code,income_year,percapita_income,county,state) VALUES (%s, %s, %s, %s, %s)"
    cur.execute(sql, row)
 
print('County income Data added successfully!')

# Use a for loop to insert each row of data into location table
for row in location_list:
    sql = "INSERT INTO location_info (postal_code,fips_code,county,city,state,legislative_district,latitude,longitude,census_tract_2020) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.execute(sql, row)
 
print('Location Info Data added successfully!')

# Use a for loop to insert each row of data into vehicle info table
for row in vehicle_types_list:
    sql = "INSERT INTO vehicle_types (vehicle_type_id,model_year,make,model,ev_type) VALUES (%s, %s, %s, %s, %s)"
    cur.execute(sql, row)
 
print('Vehicle types Data added successfully!')

# Use a for loop to insert each row of data into main vehicle table
for row in vehicles_list:
    sql = "INSERT INTO vehicles (dol_vehicle_id,vehicle_type_id,postal_code,fips_code,utility_company_id,cafv_id,vin,electric_range,base_msrp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cur.execute(sql, row)
 
print('Vehicles Data added successfully!')

# Commit the changes to the database
conn.commit()
 
# Close the cursor and the connection
cur.close()
conn.close()