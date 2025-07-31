# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 14:14:27 2025

@author: sudes
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time

# Set your ChromeDriver path here
chrome_driver_path = r"C:/Users/sudes/OneDrive/Documents/chromedriver-win64/chromedriver.exe"


# Headless browser setup
options = Options()
options.add_argument("--headless")
options.add_argument("--no-sandbox")

service = Service(chrome_driver_path)
driver = webdriver.Chrome(service=service, options=options)

# Base URL
base_url = "https://books.toscrape.com/catalogue/page-{}.html"
books_data = []

for page in range(1, 21):  # There are 50 pages
    driver.get(base_url.format(page))
    time.sleep(1)

    # Find all book elements by class name
    books = driver.find_elements(By.CLASS_NAME, "product_pod")

    for book in books:
        # Title
        h3_tag = book.find_element(By.TAG_NAME, "h3")
        a_tag = h3_tag.find_element(By.TAG_NAME, "a")
        title = a_tag.get_attribute("title")

        # Link
        link = a_tag.get_attribute("href")

        # Price
        price = book.find_element(By.CLASS_NAME, "price_color").text.strip()

        # Availability
        availability = book.find_element(By.CLASS_NAME, "availability").text.strip()

        # Rating (from class name, e.g., "star-rating Three")
        rating_class = book.find_element(By.XPATH, ".//p[contains(@class, 'star-rating')]").get_attribute("class")
        rating = rating_class.split()[-1]  # Extract "One", "Two", etc.

        # Image URL
        img_tag = book.find_element(By.TAG_NAME, "img")
        image_url = img_tag.get_attribute("src")

        books_data.append({
            "Title": title,
            "Price": price,
            "Availability": availability,
            "Rating": rating,
            "Link": link,
            "Image URL": image_url
        })

# Close browser
driver.quit()

# Save to CSV
df = pd.DataFrame(books_data)
df.to_csv("books_data.csv", index=False)

print("✅ Scraping complete. Data saved to 'books_data.csv'")

# Read the CSV file
books_data = pd.read_csv('books_data.csv')

# Add a primary key column (book_id) as integer
books_data['book_id'] = range(1, len(books_data) + 1)

# Table 1: Book Basic Information
book_info = books_data[['book_id','Title', 'Price', 'Availability', 'Rating', 'Link']].copy()

# Clean price column by removing £ symbol and converting to float
book_info['Price'] = book_info['Price'].str.replace('£', '').astype(float)

# Standardize rating to numerical values (assuming One=1, Two=2, etc.)
rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
book_info['Rating'] = book_info['Rating'].map(rating_map)

# Save to CSV 
book_info.to_csv('book_info_table.csv', index=False)

# Table 2: Book Metadata
book_metadata = books_data[['book_id','Title', 'Image URL']].copy()

# Add a column for title length (could be useful for analysis)
book_metadata['Title_Length'] = book_metadata['Title'].str.len()

# Add a column indicating if title contains special characters
book_metadata['Has_Special_Chars'] = book_metadata['Title'].str.contains(r'[^a-zA-Z0-9\s]')

# Save to CSV for HDFS
book_metadata.to_csv('book_metadata_table.csv', index=False)

print("Two tables created successfully for HDFS upload:")
print("1. book_info_table.csv - Contains basic book information")
print("2. book_metadata_table.csv - Contains book metadata and derived features")

#Design a database and store in sql and hdfs#

import os
from hdfs import InsecureClient
            
# Set up HDFS client
hdfs_client = InsecureClient('http://100.104.16.66:9870', user='master-node')
    
# List of files to upload
local_files = [
    'C:/Users/sudes/OneDrive/Documents/books_data.csv',
    'C:/Users/sudes/OneDrive/Documents/book_info_table.csv',
    'C:/Users/sudes/OneDrive/Documents/book_metadata_table.csv'
]

# HDFS target folder
hdfs_target_folder = '/Data-Engineer-1/spark-cluster/'

# Ensure HDFS target folder exists
hdfs_client.makedirs(hdfs_target_folder)

# Upload each file
for local_file in local_files:
    try:
        # Create full path for HDFS destination
        filename = os.path.basename(local_file)
        hdfs_file_path = os.path.join(hdfs_target_folder, filename)
        
        # Upload the file
        hdfs_client.upload(hdfs_file_path, local_file)
        print(f'Successfully uploaded {filename} to {hdfs_file_path}')
    except Exception as e:
        print(f'Failed to upload {filename}: {e}')


#connect to my sql    
import os
import pandas as pd
from sqlalchemy import create_engine, text
from hdfs import InsecureClient

# HDFS and MySQL connection details
hdfs_host = 'master-node'  
hdfs_port = 9870
mysql_host = '100.104.16.66'
mysql_user = 'root'
mysql_password = '123123'
mysql_db = 'books_db'
mysql_port = 3306

# Create HDFS client
hdfs_client = InsecureClient(f'http://{mysql_host}:{hdfs_port}', user='master-node')

# Set correct HDFS path (no trailing slash)
hdfs_path = '/Data-Engineer-1/spark-cluster/'

# List CSV files in HDFS
files = hdfs_client.list(hdfs_path)
print("Files in HDFS directory:", files)

# Expected files to process
target_files = ['books_data.csv', 'book_info_table.csv', 'book_metadata_table.csv']

# Verify all required files exist
missing_files = [f for f in target_files if f not in files]
if missing_files:
    raise FileNotFoundError(f"Missing files in HDFS: {missing_files}. Found: {files}")

# Function to read CSV from HDFS
def read_csv_from_hdfs(hdfs_client, hdfs_path, filename):
    with hdfs_client.read(f'{hdfs_path}/{filename}', encoding='utf-8') as f:
        return pd.read_csv(f)

# Read all CSV files from HDFS
books_data = read_csv_from_hdfs(hdfs_client, hdfs_path, 'books_data.csv')
book_info = read_csv_from_hdfs(hdfs_client, hdfs_path, 'book_info_table.csv')
book_metadata = read_csv_from_hdfs(hdfs_client, hdfs_path, 'book_metadata_table.csv')

# Create MySQL engine
engine = create_engine(f'mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}')

# Drop existing tables with foreign key constraint handling
with engine.connect() as connection:
    connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
    
    # Drop tables in reverse dependency order
    connection.execute(text("DROP TABLE IF EXISTS book_metadata;"))
    connection.execute(text("DROP TABLE IF EXISTS book_info;"))
    connection.execute(text("DROP TABLE IF EXISTS books;"))
    
    connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

# Load data into MySQL with proper table relationships
try:
    # First check if book_id exists in books_data
    if 'book_id' not in books_data.columns:
        # Add book_id column if it doesn't exist
        books_data['book_id'] = range(1, len(books_data) + 1)
    
    # Create books table with primary key
    books_data.to_sql('books', con=engine, if_exists='append', index=False)
    
    # Verify book_id exists in related tables
    if 'book_id' not in book_info.columns:
        raise ValueError("book_id column missing in book_info_table.csv")
    if 'book_id' not in book_metadata.columns:
        raise ValueError("book_id column missing in book_metadata_table.csv")
    
    # Create related tables
    book_info.to_sql('book_info', con=engine, if_exists='append', index=False)
    book_metadata.to_sql('book_metadata', con=engine, if_exists='append', index=False)
    
    # Add constraints
    with engine.connect() as connection:
        # Add primary key to books table
        connection.execute(text("ALTER TABLE books ADD PRIMARY KEY (book_id);"))
        
        # Add foreign key to book_info
        connection.execute(text("""
        ALTER TABLE book_info 
        ADD CONSTRAINT fk_book_info 
        FOREIGN KEY (book_id) REFERENCES books(book_id)
        ON DELETE CASCADE;
        """))
        
        # Add foreign key to book_metadata
        connection.execute(text("""
        ALTER TABLE book_metadata 
        ADD CONSTRAINT fk_book_metadata 
        FOREIGN KEY (book_id) REFERENCES books(book_id)
        ON DELETE CASCADE;
        """))
    
    print("All tables loaded into MySQL successfully with proper relationships")
    
except Exception as e:
    print(f"Error loading data into MySQL: {e}")
    # Rollback changes if error occurs
    with engine.connect() as connection:
        connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        connection.execute(text("DROP TABLE IF EXISTS book_metadata;"))
        connection.execute(text("DROP TABLE IF EXISTS book_info;"))
        connection.execute(text("DROP TABLE IF EXISTS books;"))
        connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
    raise

#import sys
#sys.path.append(r'C:\Users\sudes\Anaconda3\Lib\site-packages')
#from hdfs import InsecureClient