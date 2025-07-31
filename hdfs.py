import os
from hdfs import InsecureClient

# Set up HDFS client
# Connect to HDFS
hdfs_client = InsecureClient('http://100.69.136.18:9870', user='hadoop')

# Path to the folder containing CSV files on your Mac
local_csv_folder = '/Users/anshkumar/Developer/Code/Python/Data Engineering/dataset'
# Path in HDFS where you want to store the files
hdfs_target_folder = '/home/hadoop/data/nameNode/dataset'

# Ensure the HDFS target folder exists
hdfs_client.makedirs(hdfs_target_folder)

# Upload each CSV file to HDFS
for csv_file in os.listdir(local_csv_folder):
    if csv_file.endswith('.csv'):
        local_file_path = os.path.join(local_csv_folder, csv_file)
        hdfs_file_path = os.path.join(hdfs_target_folder, csv_file)
        try:
            hdfs_client.upload(hdfs_file_path, local_file_path)
            print(f'Successfully uploaded {csv_file} to {hdfs_file_path}')
        except Exception as e:
            print(f'Failed to upload {csv_file}: {e}')
