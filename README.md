# Dream-Team-Project-3
Authors: Chris Adams, Brandon Johnson, and Anna Yayloyan
<br>
<br>

## Project Overview
The goal of this project is to extract data on registered electric vehicles in the U.S. State of Washington as well as income data in U.S counties to analyze who is buying what kind of electric vehicle and where in Washington state. This data and project could be used in various applications, such as government agencies and vehicle companies. The grain of the primary dataset is individual electric vehicles registered in Washington state and the secondary dataset is income per county for a selected year.
<br>
### Ethical Considerations
Data can contain personal identifying information about individuals that could potentially result in harm being done to them and their privacy. In this poltical climate, electric vehicles are particularly politicized. To avoid this data being used against indivdual vehicle owners in any way, we blocked out most of the VIN number on the vehicles. The location info is too broad to be concenred with, so we only focused on the VIN.
<br>
<br>

## Database Overview
To store our data, we're using PostgreSQL to establish a database called 'ev_db.' <br>
<br>
ERD for Electric Vehicle Database: <br>
<img width="2485" alt="EV_DB_ERD" src="https://github.com/user-attachments/assets/49897be0-a7bb-4376-90ae-3ab921340df6">
<br>
<br>
The data will be divided into the following six tables: <br>
<img width="188" alt="Screenshot 2024-12-16 at 13 47 02" src="https://github.com/user-attachments/assets/3e170e77-3c6d-448b-9155-01aa0c4e0983" />
<br>
<br>
Here are examples of the data from the six tables using SQL statements: <br>
<img width="205" alt="Screenshot 2024-12-16 at 14 15 06" src="https://github.com/user-attachments/assets/76a1d5b9-1aa0-4ba4-bd05-69ba99d2c7cd" />
<br>
<img width="1325" alt="Screenshot 2024-12-16 at 14 14 28" src="https://github.com/user-attachments/assets/b4f803d6-bf86-4b6d-8bf2-0c07359c43b7" />
<br>
<br>
<img width="255" alt="Screenshot 2024-12-16 at 14 18 51" src="https://github.com/user-attachments/assets/4fa36d5a-310f-4735-9944-65de5b9ed44a" />
<br>
<img width="931" alt="Screenshot 2024-12-16 at 14 18 02" src="https://github.com/user-attachments/assets/70b59198-86be-47c0-9d87-652ce1c0ef39" />
<br>
<br>
<img width="281" alt="Screenshot 2024-12-16 at 14 21 25" src="https://github.com/user-attachments/assets/9ba4ede3-c936-4142-ad1f-6b3005803620" />
<br>
<img width="574" alt="Screenshot 2024-12-16 at 14 20 59" src="https://github.com/user-attachments/assets/cf9f1b3d-3d16-4915-929c-8b03c3446c20" />
<br>
<br>
<img width="251" alt="Screenshot 2024-12-16 at 14 23 35" src="https://github.com/user-attachments/assets/a74bab33-da06-4d2c-81e3-d86414aa075b" />
<br>
<img width="1276" alt="Screenshot 2024-12-16 at 14 24 02" src="https://github.com/user-attachments/assets/74e9a3e3-e6e5-42d9-a50b-6a572a3d2de4" />
<br>
<br>
<img width="252" alt="Screenshot 2024-12-16 at 14 26 14" src="https://github.com/user-attachments/assets/dbd2ce93-1903-4769-8826-3cfbd926dc68" />
<br>
<img width="488" alt="Screenshot 2024-12-16 at 14 26 41" src="https://github.com/user-attachments/assets/b4025bb8-a681-425a-a7c1-70296d6837c8" />
<br>
<br>
<img width="273" alt="Screenshot 2024-12-16 at 14 27 59" src="https://github.com/user-attachments/assets/7b80cef9-85de-40a8-af1f-475a63b0bf50" />
<br>
<img width="563" alt="Screenshot 2024-12-16 at 14 28 22" src="https://github.com/user-attachments/assets/73c94679-5c8f-44aa-a99c-a12a62822f11" />
<br>
<br>

## Repository Overview
### DB_init directory

- db_init.py <br>
This python file initiates the 'ev_db' database in PostgreSQL using 'psycopg2.' It extracts income data by county for the year the user selects, then combines that with an Electric Vehicle csv that contains data for every registered electric vehicle in Washington state. It then will divide this dataframe into six seperate dataframes and import it into six seperate tables in our 'ev_db' PostgreSQL database. <br>
Executing the program looks like this: <br>
<img width="839" alt="Screenshot 2024-12-16 at 14 39 46" src="https://github.com/user-attachments/assets/c9754ae1-b883-4c33-b077-26dbaa6137b4" />
<br>
<br>
Doing this will set the entire database up as it is displayed in the section above. <br>
<br>

- db_init_notebook.ipynb <br>
This jupyter notebook file breaks down the code in the 'db_init.py' file step by step. You can use this to view how the data is transformed through the process before it is uploaded to PostgreSQL <br>
<br>

- Output_CSVs Directory <br>
This directory holds all the data in the individual tables in csv format as they are being processed by the db_init app. <br>
<br>

- Resources Directory <br>
This directory holds the "Electric Vehicle Population Data" csv that is used as the primary dataset in our daabase
<br>

### Input_App directory

- input_app.py <br>
This python program extracts data from our 'ev_db' PostgreSQL database by asking the user to input their own custom SQL statements. It then will ask the user to make a name for csv that the data is being extracted to. The program can be looped through several times if the user chooses to run it again. <br>
<br>
Initiating the app will give you a list of all available tables and their fields like this: <br>
<img width="396" alt="Screenshot 2024-12-16 at 15 00 36" src="https://github.com/user-attachments/assets/7b56fac4-6985-453c-8d4b-25f805f938f7" />
<br>
<br>
Then it will ask you to input a SQL query and to name the CSV file <br>
<img width="687" alt="Screenshot 2024-12-16 at 15 07 30" src="https://github.com/user-attachments/assets/49bb143e-5b33-4c56-ba93-8cc8da849233" />
<br>
<br>
Finally it will display a sample of the data like this and ask if you would like to run the program again <br>
<img width="587" alt="Screenshot 2024-12-16 at 15 09 57" src="https://github.com/user-attachments/assets/d921760a-0be5-4263-a2dc-1f0f414ba3a7" />
<br>
<br>

- input_app_notebook.ipynb <br>
This jupyter notebook runs the above input program, but allows you to use the data extracted as a pandas dataframe that can be manipulated <br>
<br>

- Input_App_CSVs Directory <br>
This directory stores the CSVs from the input apps above. <br>
<br>

### Extraction directory

- db_bokeh_charts.ipynb <br>
This notebook gives examples of data being extracted from the database and then being maniplated to make into interative charts with the bokeh library. The first example of extarcted data uses the following SQL query to pull all registered CYBERTRUCKs: <br>
<br>
SELECT county, model <br>
FROM vehicles <br>
JOIN location_info <br>
ON vehicles.postal_code = location_info.postal_code <br>
JOIN vehicle_types <br>
ON vehicles.vehicle_type_id = vehicle_types.vehicle_type_id <br>
WHERE model = 'CYBERTRUCK' <br>
AND state = 'WA'; <br>
<br>
It results in the following chart: <br>
<img width="2080" alt="Screenshot 2024-12-16 at 15 26 11" src="https://github.com/user-attachments/assets/98e6791f-3c7a-4aae-a2cf-9bc0bcdf472b" />
<br>
<br>
The next example uses the following query to extarct all vehicles where the 'electric_range' is above 0: <br>
SELECT vehicles.vehicle_type_id, electric_range, model_year, make, model <br>
FROM vehicles <br>
JOIN vehicle_types <br>
ON vehicles.vehicle_type_id = vehicle_types.vehicle_type_id <br>
WHERE electric_range > 0; <br>
<br>
It results in the following chart: <br>
<img width="1577" alt="Screenshot 2024-12-16 at 15 28 21" src="https://github.com/user-attachments/assets/b8090b83-ffe5-4bd9-a79c-84aaf07ff21c" />
<br>
<br>
Bokeh is an extremely capable data visualization tool and is able to display many different types of interactive charts. For more information on Bokeh, please view their documentation here: <br>
https://docs.bokeh.org/en/latest/docs/user_guide.html <br>
<br>

- Graphs Directory <br>
This directory hold the images of the graphs above. <br>
<br>

### SQL directory

- ev_db_tables.sql <br>
This file contains the outline for the SQL tables created in the initialization step. <br>

- EV_DB_ERD.png <br>
This is the image of the ERD posted above. <br>
<br>

## Ressoures

Electric Vehicle Population Data for Washington State <br>
https://catalog.data.gov/dataset/electric-vehicle-population-data <br>
<br>
Bureau of Economic Analysis - PerCapita Income by County for 2022 <br>
https://apps.bea.gov/api/data?&UserID={INSERT_YOUR_CODE_HERE}&method=GetData&datasetname=Regional&TableName=CAINC1&LineCode=1&Year=2022&GeoFips=COUNTY&ResultFormat=json <br>
