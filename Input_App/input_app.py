import psycopg2
import pandas as pd
# Import postgres server info key
from postgres_info import user, password

while True:
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
        break
    else:
        print("Connection to ev_db established successfully")

    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Create a variable to hold all table names and column names in our database
        select_table_names = """SELECT table_name,column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'public';"""
        # Execute the SQL query above
        cursor.execute(select_table_names)
        # Fetch all data from the query execution above
        tables_rows = cursor.fetchall()
        # Assemble the data into a dataframe
        tables_rows_df = pd.DataFrame(tables_rows).sort_values(by=0)
        # Show the dataframe
        print(f'Available Tables and Columns:  {tables_rows_df}')

        # Create an input variable for users to write their own SQL query
        input_query = input("Enter a SQL query to download table as CSV: ")
        # Execute the query that holds the user selection
        cursor.execute(input_query)
        # Print statement to show user's query executed successfully 
        print(f'Query Executed Successfully: {input_query}')
        # Fetch all rows from the result
        rows = cursor.fetchall()
        # Use cursor.description to get column names from SQL query
        column_names = [desc[0] for desc in cursor.description]
        # Assemble data into a dataframe
        input_df = pd.DataFrame(rows, columns=column_names)
        # Ask for name for CSV
        csv_name = input("Enter a name for the CSV: ")
        # Save as csv to Input_App_CSVs directory
        input_df.to_csv(f"Input_App_CSVs/{csv_name}.csv", index=False)
        # Print statement to show user CSV was saved successfully
        print(f"Data successfully saved to 'Input_App_CSVs' as '{csv_name}'")
        # Close cursor
        cursor.close()
        # Show dataframe
        print(f'Results of {input_query}')
        print(input_df)
        
    # Use expect to handle error    
    except psycopg2.Error as e:
        print(f"Error: {e}")

    # Close connection
    conn.close()

    run_again = input("Do you want to run the script again? (yes/no): ").strip().lower()
    if run_again != "yes":
        print("Thank you for using our app!")
        break