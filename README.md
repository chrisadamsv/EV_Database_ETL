# Dream-Team-Project-3
Authors: Chris Adams, Brandon Johnson, and Anna Yayloyan

## Project Overview
The goal of this project is to extract data on registered electric vehicles in the U.S. State of Washington as well as income data in U.S counties to analyze who is buying what kind of electric vehicle and where in Washington state. This data and project could be used in various applications, such as government agencies and vehicle companies. The grain of the primary dataset is individual electric vehicles registered in Washington state and the secondary dataset is income per county for a selected year.

### Ethical Considerations
Data can contain personal identifying information about individuals that could potentially result in harm being done to them and their privacy. In this poltical climate, electric vehicles are particularly politicized. To avoid this data being used against indivdual vehicle owners in any way, we blocked out most of the VIN number on the vehicles. The location info is too broad to be concenred with, so we only focused on the VIN.
<br>
### ERD for Electric Vehicle Database
<img width="2485" alt="EV_DB_ERD" src="https://github.com/user-attachments/assets/49897be0-a7bb-4376-90ae-3ab921340df6">
<br>
<br>
<br>
<br>
Resources: <br>
Electric Vehicle Population Data for Washington State <br>
https://catalog.data.gov/dataset/electric-vehicle-population-data <br>

Bureau of Economic Analysis - PerCapita Income by County for 2022 <br>
https://apps.bea.gov/api/data?&UserID={INSERT_YOUR_CODE_HERE}&method=GetData&datasetname=Regional&TableName=CAINC1&LineCode=1&Year=2022&GeoFips=COUNTY&ResultFormat=json <br>
