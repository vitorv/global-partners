import pandas as pd
from sqlalchemy import create_engine
import urllib
import os

# --- Configurations ---
SERVER = 'database-global-partners.c0fcqacqwp5x.us-east-1.rds.amazonaws.com'
DATABASE = 'GlobalPartnersDB'
USERNAME = 'admin'
# TODO: Replace with your actual database password
PASSWORD = 'b5kBlDX6M14Rv0' 

# Build the connection string using pyodbc
driver = '{ODBC Driver 18 for SQL Server}'
odbc_str = f'DRIVER={driver};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;'
connect_uri = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"

print("Connecting to RDS SQL Server...")
engine = create_engine(connect_uri)

# --- Path to CSVs ---
data_dir = os.path.join("docs", "source_data")
files = {
    'date_dim': 'date_dim.csv',
    'order_item_options': 'order_item_options.csv',
    'order_items': 'order_items.csv'
}

for table_name, file_name in files.items():
    file_path = os.path.join(data_dir, file_name)
    print(f"\nProcessing {file_name}...")
    
    # Read CSV
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} rows. Uploading to '{table_name}' table...")
    
    # Upload to SQL Server
    # Note: 'replace' will drop the table if it exists and recreate it.
    # Use 'append' if you are inserting into existing schemas.
    df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=10000)
    
    print(f"Successfully uploaded into {table_name}!")

print("\nAll data uploaded successfully!")
