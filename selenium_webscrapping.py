# -*- coding: utf-8 -*-
"""
Created on Tue Jul  8 11:45:36 2025

@author: sudes
"""
import pandas as pd
import selenium
print(selenium.__version__)
#import subprocess
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time

#set up
web = 'https://store.steampowered.com/search/?tags=19'
path = r"C:/Users/sudes/OneDrive/Documents/chromedriver-win64/chromedriver.exe"
service = Service(executable_path=path)
driver = webdriver.Chrome(service=service)
driver.get("https://store.steampowered.com/search/?tags=19") #to open the page link for action game
time.sleep(5)  # Wait for page to load

# Scroll to load more games 
for _ in range(3):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)


#to check if the .exe file path is the same as getting error
import os
print(os.path.exists('C:/Users/sudes/OneDrive/Documents/chromedriver-win64/chromedriver.exe'))

#X-path ,using contains as multiple attribute values, attribute-productlistitem, list with class, thats why@)
    #scrape the product item
game_items = driver.find_elements(By.XPATH, '//div[@id="search_resultsRows"]/a') #for selinium 4, locating all the items in the page
print("Found:", len(game_items)) #showing data count
#products = driver.find_elements_by_xpath('//li[contains(@class, "productListItem")]') #(for selinium 3)

titles = []
release_dates = []
final_prices = []
review_scores = []
image_urls = []


#find_elements function inside the products is always return list 
for item in game_items:
    title = item.find_element(By.XPATH, './/span[@class="title"]').text
    release = item.find_element(By.XPATH, './/div[contains(@class,"search_released")]').text
    price_el = item.find_element(By.XPATH, './/div[contains(@class,"search_price")]')
    price_text = price_el.text.strip().split('\n')[-1]  # Get the last visible price
    review = item.find_element(By.XPATH, './/div[@class="col search_reviewscore responsive_secondrow"]')
    try:
        image_el = item.find_element(By.XPATH, './/div[@class="col search_capsule"]/img')
        image_url = image_el.get_attribute('src')
    except:
        image_url = 'N/A'
    
    titles.append(title)
    release_dates.append(release)
    final_prices.append(price_text)
    review_scores.append(review)
    image_urls.append(image_url)
    
    
driver.quit()

#save the scrape data in csv file

df = pd.DataFrame({'Title': titles,'Release Date': release_dates,'Final Price': final_prices, 'Image URL': image_urls})

#df_books = pd.DataFrame({'title' : book_title, 'author' : book_author, 'length' : book_length})
df.to_csv('steam_action_games.csv', index=False)

# to check where the file steam_action_games.csv file got saved
import os

file_path = os.path.abspath('steam_action_games.csv')
print("Saved to:", file_path)

#Design a database and store in sql and hdfs#

import os
from hdfs import InsecureClient

# Set up HDFS client
# Connect to HDFS
hdfs_client = InsecureClient('http://100.104.16.66:9870', user='master-node')

# Path to the folder containing CSV files on your system
local_csv_folder ='C:/Users/sudes/OneDrive/Documents/steam_action_games.csv'
# Path in HDFS where you want to store the files
hdfs_target_folder = '/Data-Engineer-1/spark-cluster'

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
            
#connect to my sql
import pandas as pd
from sqlalchemy import create_engine
from hdfs import InsecureClient
from sqlalchemy import text

# HDFS and MySQL connection details
hdfs_host = 'master-node'  # e.g., 'localhost'
hdfs_port = 9870
mysql_host = '100.104.16.66'
mysql_user = 'root'
mysql_password = '123123'
mysql_db = 'steam_db'
mysql_port = 3306

# Create HDFS client
hdfs_client = InsecureClient('http://100.104.16.66:9870', user='master-node')

# List files in HDFS directory
hdfs_path = '/Data-Engineer-1/spark-cluster/'
files = hdfs_client.list(hdfs_path)
print("Files in HDFS directory:", files)

# Function to read CSV from HDFS
def read_csv_from_hdfs(hdfs_client, hdfs_path, filename):
    with hdfs_client.read(f'{hdfs_path}/{filename}', encoding='utf-8') as f:
        return pd.read_csv(f)

# Read CSV files
steam_action_games_2 = read_csv_from_hdfs(hdfs_client, hdfs_path, 'steam_action_games_2.csv')


# Create MySQL engine
engine = create_engine(f'mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}')

# Drop foreign key constraints
with engine.connect() as connection:
    connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
    connection.execute(text("DROP TABLE IF EXISTS games;"))

# Load data into MySQL
steam_action_games_2.to_sql('games', con=engine, if_exists='replace', index=False)

# Re-enable foreign key checks
with engine.connect() as connection:
    connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

print("Data loaded into MySQL successfully")


import sys
print(sys.executable)

#import sys
#sys.path.append(r'C:\Users\sudes\Anaconda3\Lib\site-packages')
#from hdfs import InsecureClient