import pyhdfs
import pandas as pd
from sqlalchemy import create_engine
from hdfs import InsecureClient
from sqlalchemy import text

# HDFS and MySQL connection details
hdfs_host = 'master-node'  # e.g., 'localhost'
hdfs_port = 9870
mysql_host = '192.168.64.11'
mysql_user = 'root'
mysql_password = '9084Ubuntu#'
mysql_db = 'PropertyData'
mysql_port = 3306

# Create HDFS client
hdfs_client = InsecureClient('http://100.69.136.18:9870', user='hadoop')

# List files in HDFS directory
hdfs_path = '/home/hadoop/data/nameNode/dataset'
files = hdfs_client.list(hdfs_path)
print("Files in HDFS directory:", files)

# Function to read CSV from HDFS
def read_csv_from_hdfs(hdfs_client, hdfs_path, filename):
    with hdfs_client.read(f'{hdfs_path}/{filename}', encoding='utf-8') as f:
        return pd.read_csv(f)

# Read CSV files
place_df = read_csv_from_hdfs(hdfs_client, hdfs_path, 'place.csv')
property_type_df = read_csv_from_hdfs(hdfs_client, hdfs_path, 'propertytype.csv')
property_df = read_csv_from_hdfs(hdfs_client, hdfs_path, 'propertydata.csv')
retail_store_df = read_csv_from_hdfs(hdfs_client, hdfs_path, 'retailstore.csv')
crime_data_df = read_csv_from_hdfs(hdfs_client, hdfs_path, 'crimedata.csv')
pharmacy_data_df = read_csv_from_hdfs(hdfs_client, hdfs_path, 'pharmacy.csv')

# Create MySQL engine
engine = create_engine(f'mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}')

# Drop foreign key constraints
with engine.connect() as connection:
    connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
    connection.execute(text("DROP TABLE IF EXISTS Property, RetailStore, CrimeData, Pharmacy, Place, PropertyType;"))

# Load data into MySQL
place_df.to_sql('Place', con=engine, if_exists='replace', index=False)
property_type_df.to_sql('PropertyType', con=engine, if_exists='replace', index=False)
property_df.to_sql('Property', con=engine, if_exists='replace', index=False)
retail_store_df.to_sql('RetailStore', con=engine, if_exists='replace', index=False)
crime_data_df.to_sql('CrimeData', con=engine, if_exists='replace', index=False)
pharmacy_data_df.to_sql('Pharmacy', con=engine, if_exists='replace', index=False)

# Re-enable foreign key checks
with engine.connect() as connection:
    connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

print("Data loaded into MySQL successfully")
