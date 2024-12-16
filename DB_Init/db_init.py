# Import python libraries
import psycopg2
import pandas as pd
import requests
import json
from datetime import datetime

# Import postgres server info key
from postgres_info import user, password
from api_keys import bea_key

# Print statement introducing the app
print('')
print('This App is going to create and add data to a database in PostgreSQL called "ev_db" to hold data for electric vehicles registered in Washington State and the income data for all associated counties in the year selected.')
print('')

# Run a while True statement to ask if the user wants to initialize the app.
while True:

    # Ask the user if they want to continue with input statement
    user_input = input("Do you want to continue? (yes/no): ").strip().lower()

    # End app if user types no
    if user_input == "no":
        print('')
        print("Initialization canceled. Thank you for using our app.")
        print('')
        break
    
    # Initalize app if user types yes
    elif user_input == "yes":

        #############################################################
        # Import income data by county from BEA API
        #############################################################

        # Create a 'while True:' statement to collect the year variable in the appropriate format
        while True:
            # Create a variable to hold the current year as an integer
            current_year = int(datetime.now().year)
            # Create a variable to store the user input for the year to be extracted from the API
            print('')
            year_parameter = input(f"Enter a year between 2010 and {current_year} to pull for county income from the BEA API (type 'quit' to terminate program): ")
            # Use an 'if' statement to ensure the input is a 4 character digit 
            if year_parameter.isdigit() and len(year_parameter) == 4:
                # Convert user input variable to integer if meets format requirements
                year_parameter_int = int(year_parameter)
                # Use another 'if' statement to ensure the input year is between 2010 and the curent year. If so, initialize script
                if 2010 <= year_parameter_int < current_year:

                    # Create variables for the API call to get Income data by county
                    base_url = 'https://apps.bea.gov/api/data'
                    tablename = 'CAINC1' #per capita income by county
                    linecode = '1' 

                    # Print statement confirming that API is being extracted with selected year
                    print('')
                    print(f'Pulling Income data from API for year {year_parameter}...')

                    # Create URL to Bureau of Economic Analysis for per capita income by county by year
                    income_url = f'{base_url}?&UserID={bea_key}&method=GetData&datasetname=Regional&TableName={tablename}&LineCode={linecode}&Year={year_parameter}&GeoFips=COUNTY&ResultFormat=json'

                    # Pull the response into the notebook
                    income_json = requests.get(income_url).json()

                    # Print statement to confirm that income data was eextracted successfully
                    print('')
                    print(f'Income data for year {year_parameter} extracted successfully!')
                    print('')

                    # Display the result as a dataframe with 'json_normalize()' using only relevant columns 
                    json_df = pd.json_normalize(income_json['BEAAPI']['Results']['Data'])

                    # Create a new dataframe with columns: 'GeoFips','GeoName','TimePeriod', and 'DataValue'
                    income_df = json_df[['GeoFips','GeoName','TimePeriod','DataValue']]

                    # Rename columns
                    income_df = income_df.rename(columns={'GeoFips':'FIPS code','TimePeriod':'Income Year','GeoName':'County & State','DataValue':'PerCapita Income'})

                    # Format 'Year' column to be a string
                    income_df['Income Year'] = income_df['Income Year'].astype(str)

                    # Format 'FIPS code' column to be a string 
                    income_df['FIPS code'] = income_df['FIPS code'].astype(str)
                    # Use 'lambda' function to add leading zero back to the code
                    income_df['FIPS code'] = income_df['FIPS code'].apply(lambda x: x.zfill(5))

                    # Use regex to extract the county from the 'County & State' field
                    income_df['County'] = income_df['County & State'].str.extract(r'^([^,]+)')
                    # Use regex to extract the state from the 'County & State' field
                    income_df['State'] = income_df['County & State'].str.extract(r',([^,]+)$')
                    # Remove '*' from the end of the state value if it exists
                    income_df['State'] = income_df['State'].str.rstrip('*')
                    # Remove ' ' from the beginning of the state value if it exists
                    income_df['State'] = income_df['State'].str.lstrip()
                    # Drop old 'County & State' column
                    income_df = income_df.drop(columns=['County & State'])

                    # Convert PerCapita Income to Integer
                    income_df['PerCapita Income'] = income_df['PerCapita Income'].astype(int)
                    # Divide by hundred to get true value then convert to float
                    income_df['PerCapita Income'] = (income_df['PerCapita Income']/100).astype(float)

                    #############################################################
                    # Import EV CSV data into python
                    #############################################################

                    # Upload 'Electric Vehicle Population Data' csv to dataframe
                    main_df = pd.read_csv("Resources/Electric_Vehicle_Population_Data.csv")

                    # Drop rows where the postal code is 'NaN'
                    main_df = main_df.dropna(subset=['Postal Code'])

                    # Convert 'DOL Vehicle ID' and 'Model Year' columns to a string
                    main_df[['DOL Vehicle ID','Model Year']] = main_df[['DOL Vehicle ID','Model Year']].astype(str)

                    # Convert '2020 Census Tract' and 'Postal Code' columns to a string from a float
                    main_df[['2020 Census Tract','Postal Code']] = main_df[['2020 Census Tract','Postal Code']].astype(int).astype(str)

                    # Convert 'Legislative District' column to string and then remove decimal if exists with regex
                    main_df['Legislative District'] = main_df['Legislative District'].astype(str).str.replace(r'\.0$', '', regex=True)
                    # Replace 'nan' in Legislative District with empty value
                    main_df['Legislative District'] = main_df['Legislative District'].str.replace('nan','')

                    # Replace the first 6 charcters in the VIN column with 'xxxxxx' to help protect privacy
                    main_df['VIN (1-10)'] = main_df['VIN (1-10)'].str.replace(r'^.{6}', 'xxxxxx', regex=True)

                    # Extract longitude from 'Vehicle Location' column and convert to float
                    main_df['Longitude'] = main_df['Vehicle Location'].str.extract(r'(-\d+\.\d+)', expand=False).astype(float)
                    # Extract latitude from 'Vehicle Location' column and convert to float
                    main_df['Latitude'] = main_df['Vehicle Location'].str.extract(r' (\d+\.\d+)', expand=False).astype(float)

                    # Drop 'Vehicle Location' column
                    main_df = main_df.drop('Vehicle Location', axis=1)

                    # Merge income data frome above dataframe 'income_df'
                    new_main_df = pd.merge(main_df, income_df, on=['County','State'])

                    #############################################################
                    # Create location Dataframe
                    #############################################################

                    # Create dataframe with location based columns from above dataframe
                    vehicle_location_df = new_main_df[['Postal Code','FIPS code','County','City','State','Legislative District','Latitude','Longitude','2020 Census Tract']]

                    # Get unique values by dropping postal code duplicates
                    location_df = vehicle_location_df.drop_duplicates(subset='Postal Code').sort_values(by=['Postal Code'], ascending=False)

                    # Save as a new csv
                    location_df.to_csv("Output_CSVs/location.csv", index=False)

                    #############################################################
                    # Create vehicle info Dataframe
                    #############################################################

                    # Create dataframe with vehicle info columns. Sort by year and drop duplicates
                    vehicle_types_df = main_df[['Model Year','Make','Model','Electric Vehicle Type']].sort_values(by=['Model Year','Make','Model'], ascending=True).drop_duplicates()

                    # Save as csv and reupload to reset index
                    vehicle_types_df.to_csv("Output_CSVs/vehicle_types.csv", index=False)
                    vehicle_types_df = pd.read_csv("Output_CSVs/vehicle_types.csv")

                    # Re-convert Model Year column to a string
                    vehicle_types_df['Model Year'] = vehicle_types_df['Model Year'].astype(str)

                    # Create a new column to hold new vehicle id. Use the columns index and add one digit
                    vehicle_types_df['Vehicle Type ID'] = vehicle_types_df.index+1
                    # Use 'lambda' function to add leading zeros to ID to ensure it is four digits
                    vehicle_types_df['Vehicle Type ID'] = vehicle_types_df['Vehicle Type ID'].astype(str).apply(lambda x: x.zfill(4))
                    # Add 'vm' to the front of every ID
                    vehicle_types_df['Vehicle Type ID'] = vehicle_types_df['Vehicle Type ID'].apply(lambda x: f'vm{x}')

                    # Move new ID column to the front of dataframe
                    vehicle_types_df = vehicle_types_df[['Vehicle Type ID'] + [col for col in vehicle_types_df.columns if col != 'Vehicle Type ID']]

                    # Resave as CSV
                    vehicle_types_df.to_csv("Output_CSVs/vehicle_types.csv", index=False)

                    #############################################################
                    # Create CAFV Eligibility Dataframe
                    #############################################################

                    # Create CAFV Eligibility table using 'CAFV eligibility' column and drop duplicates
                    cafv_df = main_df[['Clean Alternative Fuel Vehicle (CAFV) Eligibility']].drop_duplicates()
                    # Save as CSV and reupload to reset index
                    cafv_df.to_csv("Output_CSVs/csfv_eligibility.csv", index=False)
                    cafv_df = pd.read_csv("Output_CSVs/csfv_eligibility.csv")

                    # Create new column with index for ID and add 1
                    cafv_df['CAFV Eligibility ID'] = cafv_df.index+1
                    # Add 'cafv' to the front of the id numbers using 'lambda' function
                    cafv_df['CAFV Eligibility ID'] = cafv_df['CAFV Eligibility ID'].apply(lambda x: f'cafv{x}')

                    # Move ID First
                    cafv_df = cafv_df[['CAFV Eligibility ID','Clean Alternative Fuel Vehicle (CAFV) Eligibility']]

                    # Resave as CSV
                    cafv_df.to_csv("Output_CSVs/csfv_eligibility.csv", index=False)

                    #############################################################
                    # Create Utility Companies Dataframe
                    #############################################################

                    # Create a new dataframe with the utility company name and drop duplicates
                    utilities_df = main_df[['Electric Utility']].drop_duplicates()

                    # Save as CSV and reopen to reset index
                    utilities_df.to_csv("Output_CSVs/utilities.csv", index=False)
                    utilities_df = pd.read_csv("Output_CSVs/utilities.csv")

                    # Create new column with index for ID and add 1
                    utilities_df['Electric Company ID'] = utilities_df.index+1

                    # Use 'lambda' function to add leading zeros to ID
                    utilities_df['Electric Company ID'] = utilities_df['Electric Company ID'].astype(str).apply(lambda x: x.zfill(3))
                    # Add 'cafv' to the front of the id numbers
                    utilities_df['Electric Company ID'] = utilities_df['Electric Company ID'].apply(lambda x: f'uc{x}')

                    # Move ID column First
                    utilities_df = utilities_df[['Electric Company ID','Electric Utility']]

                    # Resave as CSV
                    utilities_df.to_csv("Output_CSVs/utilities.csv", index=False)

                    #############################################################
                    # Re-create county income Dataframe
                    #############################################################

                    # Remake income_df without county and state fields
                    income_df = income_df[['FIPS code','Income Year','PerCapita Income']]

                    # Resave to existing csv 
                    income_df.to_csv('Output_CSVs/income_data.csv',index=False)

                    #############################################################
                    # Create main vehicle Dataframe
                    #############################################################

                    # Merge vehicle types dataframe with the 'new_main_df' 
                    vehicle_df = pd.merge(new_main_df,vehicle_types_df, on=['Model Year','Make','Model','Electric Vehicle Type'])

                    # Merge the CAFV eligibility dataframe with the new merged dataframe above
                    vehicle_df = pd.merge(vehicle_df,cafv_df, on='Clean Alternative Fuel Vehicle (CAFV) Eligibility')

                    # Merge the Utilities dataframe with the new merged dataframe above to create one large dataframe
                    vehicle_df = pd.merge(vehicle_df,utilities_df, on='Electric Utility')

                    # Remake DataFrame with needed columns only
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

                    # Use a 'try' statement to handle errors connecting to the database
                    try:
                        # Connect to the database
                        conn = psycopg2.connect(
                            host="127.0.0.1",
                            port="5432",
                            user=user,
                            password=password,
                            database="postgres")
                        # Make sure autocommit is 'True' to commit new database successfully
                        conn.autocommit = True
                    # Use 'except' statement to handle and display an error connect to PostgreSQL
                    except psycopg2.Error as e:
                        print('-'*60)
                        print("Error connecting to PostgreSQL:")
                        print(e)
                    # Use 'else' statemnt is connection if successful
                    else:
                        print('-'*60)
                        print("Connection established to PostgreSQL successfully")

                    # Create cursor
                    cur = conn.cursor()

                    # Query to create 'ev_db' database
                    sql = '''CREATE DATABASE ev_db;'''

                    # Use 'try' statement to execute above SQL statement
                    print('-'*60)
                    try:
                        cur.execute(sql)
                        print("'Ev_db' database successfully created!")
                    # Use 'except' statement to handle if database already exists
                    except:
                        print("'Ev_db' database already exists! Execute command aborted...")


                    # Close cursor
                    cur.close()

                    # Close connection
                    conn.close()

                    #############################################################
                    # Create new tables and add data in PostGRES
                    #############################################################
                    # Use a 'try' statement to handle errors connecting to the database
                    try:
                        # Connect to the database
                        conn = psycopg2.connect(
                            host="127.0.0.1",
                            port="5432",
                            user=user,
                            password=password,
                            database="ev_db")
                    # Use 'except' statement to handle and display an error connect to PostgreSQL
                    except psycopg2.Error as e:
                        print('-'*60)
                        print("Error connecting to 'ev_db' database:")
                        print(e)
                    # Use 'else' statemnt if connection is successful
                    else:
                        print('-'*60)
                        print("Connection to 'ev_db' database established successfully")

                    # Create cursor
                    cur = conn.cursor() 

                    # SQL queries to drop tables if already exists. Use 'CASCADE' to ensure all dependent tables are dropped
                    drop_cafv_table_query = '''DROP TABLE IF EXISTS cafv_eligibility CASCADE;'''
                    drop_utilities_query = '''DROP TABLE IF EXISTS utility_companies CASCADE'''
                    drop_income_table_query = '''DROP TABLE IF EXISTS county_income CASCADE'''
                    drop_location_info_query = '''DROP TABLE IF EXISTS location_info CASCADE;'''
                    drop_vehicle_types_query = '''DROP TABLE IF EXISTS vehicle_types CASCADE;'''
                    drop_vehicles_query = '''DROP TABLE IF EXISTS vehicles CASCADE;'''

                    # SQL queries to create tables for our data
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
                            percapita_income DECIMAL(10,2)
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
                            vin VARCHAR(10) NOT NULL
                            CHECK (vin LIKE 'xxxxxx%'),
                            electric_range DECIMAL(10,1),
                            base_msrp DECIMAL(10,2)
                        );
                        """

                    # Execute Drop table query 
                    cur.execute(drop_cafv_table_query)
                    # Execute the CREATE TABLE query
                    cur.execute(create_cafv_table_query)
                    print('-'*60)
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

                    # Execute Drop table query 
                    cur.execute(drop_location_info_query)
                    # Execute the CREATE TABLE query
                    cur.execute(create_location_info_query)
                    print("Location info table created or recreated successfully!")

                    # Execute Drop table query 
                    cur.execute(drop_vehicle_types_query)
                    # Execute the CREATE TABLE query
                    cur.execute(create_vehicle_types_query)
                    print("Vehicle types table created or recreated successfully!")

                    # Execute Drop table query 
                    cur.execute(drop_vehicles_query)
                    # Execute the CREATE TABLE query
                    cur.execute(create_vehicles_query)
                    print("Vehicles table created or recreated successfully!")

                    # Print statement showing data is loading
                    print('-'*60)
                    print('Loading CAFV Eligibility data into table...')
                    # Use a 'for' loop to insert each row of data into 'cafv_eligibility' table
                    for row in cafv_list:
                        sql = "INSERT INTO cafv_eligibility (cafv_id,cafv_eligibility) VALUES (%s, %s)"
                        cur.execute(sql, row)
                    # Print statment showing data loaded successfully
                    print('CAFV Elgibility data added successfully!')

                    # Print statement showing data is loading
                    print('-'*60)
                    print('Loading Utility Company data into table...')
                    # Use a for loop to insert each row of data into location table
                    for row in utilities_list:
                        sql = "INSERT INTO utility_companies (utility_company_id,utility_company_name) VALUES (%s, %s)"
                        cur.execute(sql, row)
                    # Print statment showing data loaded successfully
                    print('Utility Company data added successfully!')

                    # Print statement showing data is loading
                    print('-'*60)
                    print('Loading County Income data into table...')
                    # Use a for loop to insert each row of data into location table
                    for row in income_list:
                        sql = "INSERT INTO county_income (fips_code,income_year,percapita_income) VALUES (%s, %s, %s)"
                        cur.execute(sql, row)
                    # Print statment showing data loaded successfully
                    print('County Income data added successfully!')

                    # Print statement showing data is loading
                    print('-'*60)
                    print('Loading Location Info data into table...')
                    # Use a for loop to insert each row of data into location table
                    for row in location_list:
                        sql = "INSERT INTO location_info (postal_code,fips_code,county,city,state,legislative_district,latitude,longitude,census_tract_2020) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cur.execute(sql, row)
                    # Print statment showing data loaded successfully
                    print('Location Info data added successfully!')

                    # Print statement showing data is loading
                    print('-'*60)
                    print('Loading Vehicle Types data into table...')
                    # Use a for loop to insert each row of data into vehicle info table
                    for row in vehicle_types_list:
                        sql = "INSERT INTO vehicle_types (vehicle_type_id,model_year,make,model,ev_type) VALUES (%s, %s, %s, %s, %s)"
                        cur.execute(sql, row)
                    # Print statment showing data loaded successfully
                    print('Vehicle Types data added successfully!')

                    # Print statement showing data is loading
                    print('-'*60)
                    print('Loading Vehicle data into table...')
                    # Use a for loop to insert each row of data into main vehicle table
                    for row in vehicles_list:
                        sql = "INSERT INTO vehicles (dol_vehicle_id,vehicle_type_id,postal_code,fips_code,utility_company_id,cafv_id,vin,electric_range,base_msrp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cur.execute(sql, row)
                    # Print statment showing data loaded successfully
                    print('Vehicle data added successfully!')

                    # Commit all the changes to the database
                    conn.commit()

                    # Close the cursor
                    cur.close()

                    # Close the connection
                    conn.close()

                    # Print when database initialization is done
                    print('-'*60)
                    print('')
                    print('Database initalization is complete. Please check ev_db database in PostGRES to ensure data loaded correctly. Thank you for using our app.')
                    print('')
                    # End the entire script
                    exit()

                # Use an 'else' statement to handle error if input is not between 2010 and the previous year  
                else:
                    print('')
                    print(f"Invalid input. Please choose a year after 2009, but before {current_year}")
            
            # Use an 'elif' statement to end app id user types 'quit' in input above
            elif year_parameter == 'quit':
                print('')
                print("Thank you for using our app.")
                print('')
                # End entire program
                exit()
            
            # Use 'else' statement to handle error for invalid year format
            else:
                print('')
                print("Invalid input. Please enter exactly four digits.")
            
    # Give error if user doesn't type yes or no in initial input
    else:
        print('')
        print("Invalid input. Please enter 'yes' or 'no'.")
