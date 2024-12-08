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
        # Write your SQL queries
        input_query = input("Enter a SQL query to download table as CSV: ")
        # Execute the query
        cursor.execute(input_query)
        # Fetch all rows from the result
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        input_df = pd.DataFrame(rows, columns=column_names)
        # Ask for name for CSV
        csv_name = input("Enter a name for the CSV: ")
        # Save as csv to Input_App_CSVs directory
        input_df.to_csv(f"Input_App_CSVs/{csv_name}.csv", index=False)
        # Close cursor
        cursor.close()
    # Use expect to handle error    
    except psycopg2.Error as e:
        print(f"Error: {e}")

    # Close connection
    conn.close()

    run_again = input("Do you want to run the script again? (yes/no): ").strip().lower()
    if run_again != "yes":
        print("Thank you for using our app!")
        break