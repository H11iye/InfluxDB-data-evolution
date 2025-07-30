import os
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv # Import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- InfluxDB Configuration ---
# Get credentials from environment variables
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# --- CSV Output Configuration ---
OUTPUT_CSV_FILE = "influxdb_data.csv"

# --- InfluxDB Query ---
# This Flux query is specifically tailored to extract '_time', 'current_A', and 'producing_water' values.
# It filters by measurement, uid, and specifically the 'current_A' OR 'producing_water' fields.
# The 'keep' function ensures only the necessary columns (_time, _value, _field) are returned from InfluxDB.
QUERY = f'''
from(bucket: "{INFLUXDB_BUCKET}")
  |> range(start: -7d) // Query data from the last 7 days. Adjust as needed (e.g., -1h, or specific timestamps)
  |> filter(fn: (r) => r["_measurement"] == "awg_data_full")
  |> filter(fn: (r) => r["uid"] == "353636343034510C003F0046")
  |> filter(fn: (r) => r["_field"] == "current_A" or r["_field"] == "producing_water") # Filter to get both fields
  |> keep(columns: ["_time", "_value", "_field"]) # Keep essential columns for pivoting
  |> yield(name: "mean")
'''

def extract_and_save_to_csv():
    """
    Connects to InfluxDB, executes a Flux query, and saves the results to a CSV file.
    The output CSV will contain '_time', 'current_A', and 'producing_water' columns.
    """
    # Basic validation for environment variables
    if not all([INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET]):
        print("Error: One or more InfluxDB environment variables are not set.")
        print("Please ensure INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, and INFLUXDB_BUCKET are defined in your .env file.")
        return

    print(f"Connecting to InfluxDB at: {INFLUXDB_URL}")
    try:
        # Initialize InfluxDB client using a 'with' statement for automatic closing
        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG, timeout=30000) as client:
            query_api = client.query_api()

            print(f"Executing Flux query for bucket '{INFLUXDB_BUCKET}'...")
            print("Query:\n", QUERY)

            # Execute the query
            tables = query_api.query(QUERY, org=INFLUXDB_ORG)

            data = []
            # Iterate through the tables and records to collect data
            for table in tables:
                for record in table.records:
                    # Each record is a dictionary-like object.
                    # We convert it to a standard dictionary to append to our list.
                    data.append(record.values)

            if not data:
                print("No data found for the given query. CSV file will not be created.")
                return

            # Convert the collected data into a Pandas DataFrame
            df = pd.DataFrame(data)

            # --- Post-processing for desired output ---
            # Ensure '_time', '_value', and '_field' are present for pivoting
            if not all(col in df.columns for col in ['_time', '_value', '_field']):
                print("Warning: Expected '_time', '_value', or '_field' columns not found in query results. Cannot pivot.")
                # Fallback to saving whatever data was found, without pivoting
                df.to_csv(OUTPUT_CSV_FILE, index=False)
                print(f"Data extracted and saved to '{OUTPUT_CSV_FILE}' (unpivoted due to missing columns).")
                return

            # Pivot the DataFrame to have 'current_A' and 'producing_water' as separate columns
            # 'index' will be the column that defines rows (e.g., time)
            # 'columns' will be the column whose unique values become new column headers
            # 'values' will be the data that populates the new columns
            # 'aggfunc' specifies how to handle multiple values if they exist for the same time/field combination
            df_pivot = df.pivot_table(index='_time', columns='_field', values='_value', aggfunc='first')

            # Reset index to make '_time' a regular column again instead of the DataFrame index
            df_pivot = df_pivot.reset_index()

            # Remove the 'columns' name that pivot_table might add for cleaner output
            df_pivot.columns.name = None

            # Ensure the desired columns exist and reorder them for consistency
            final_cols = ['_time', 'current_A', 'producing_water']
            # Filter for only the columns that actually exist in the pivoted DataFrame
            existing_final_cols = [col for col in final_cols if col in df_pivot.columns]
            df = df_pivot[existing_final_cols]


            # Save the DataFrame to a CSV file
            df.to_csv(OUTPUT_CSV_FILE, index=False)
            print(f"Data successfully extracted and saved to '{OUTPUT_CSV_FILE}'")

    except Exception as e:
        print(f"An error occurred: {e}")
    # The 'with' statement handles closing the client, so no 'finally' block for client.close() is needed.

if __name__ == "__main__":
    extract_and_save_to_csv()