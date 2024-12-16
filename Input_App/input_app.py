# Import python libraries
import psycopg2
import pandas as pd

# Import postgres server info key
from postgres_info import user, password


# Use a 'try' statement to attempt to establish a connection to the PostgreSQL database 
try:

    # Connect to the database using login info stored in 'postgres_info.py'
    conn = psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        user=user,
        password=password,
        database="ev_db"
    )

# Use 'except' statement to handle errors connenting to the database
except psycopg2.Error as e:
    print('')
    print("Error connecting to the 'ev_db' database:")
    print(e)

# Use 'else' statement to continue program if connection is successful
else:
    print('')
    print("Connection to 'ev_db' database established successfully")
    
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
    print('')
    print(f'Available Tables and Columns:  {tables_rows_df}')
    
    # Close cursor 
    cursor.close()

    # Close the connection to the database
    conn.close()


# Create a 'while True:' statement to allow user to run script again until they quit
while True:

    # Create a 'try' statement to attempt to reconnect to database
    try:

        # Reconnect to the database
        conn = psycopg2.connect(
            host="127.0.0.1",
            port="5432",
            user=user,
            password=password,
            database="ev_db"
        )
    
    # Use 'except' statement to handle errors connenting to the database
    except psycopg2.Error as e:
        print('')
        print("Error reconnecting to the 'ev_db' database:")
        print(e)
        # Break 'while True:' loop 
        break

    # Use 'else' statement to continue program if reconnection is successful
    else:
        print('')
        print("Reconnection to 'ev_db' database established successfully")


    # Create another 'try' statement to run user selected SQL query and handle errors that occuer
    try:
        
        # Recreate the cursor
        cursor = conn.cursor()

        # Create an input variable for users to write their own SQL query
        print('')
        input_query = input("Enter a SQL query to download table as CSV: ")

        # Execute the query that holds the user selection
        cursor.execute(input_query)
        
        # Print statement to show user's query executed successfully 
        print('')
        print(f'Query Executed Successfully: {input_query}')
        
        # Fetch all rows from the result
        rows = cursor.fetchall()
        
        # Use cursor.description to get column names from SQL query
        column_names = [desc[0] for desc in cursor.description]
        
        # Assemble data into a dataframe
        input_df = pd.DataFrame(rows, columns=column_names)
        
        # Ask for name for CSV
        print('')
        csv_name = input("Enter a name for the CSV: ")
        
        # Save as csv to Input_App_CSVs directory
        input_df.to_csv(f"Input_App_CSVs/{csv_name}.csv", index=False)
        
        # Print statement to show user CSV was saved successfully
        print('')
        print(f"Data successfully saved to 'Input_App_CSVs' as '{csv_name}'")
        
        # Close the cursor
        cursor.close()

        # Show dataframe
        print('')
        print(f'Results of {input_query}')
        print(input_df)
        print('')

    # Use expect to handle error    
    except psycopg2.Error as e:
        print('')
        print(f"Error: {e}")


    # Close connection
    conn.close()

    # Use an input variable to ask if the user if they want to run the program again
    run_again = input("Do you want to run the script again? (yes/no): ").strip().lower()

    # Create an 'if' statement to handle user typing anything but 'yes'
    if run_again != "yes":
        print('')
        print("Thank you for using our app!")
        print('')
        # Close connection
        conn.close()
        # Break the program if user does not type 'yes'
        break