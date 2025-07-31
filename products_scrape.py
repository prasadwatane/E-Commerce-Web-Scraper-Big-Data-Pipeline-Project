# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 20:02:18 2025

@author: sudes
"""
import os
import requests
import time
import random
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument("--headless")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_argument("--start-maximized")

path = r"C:/Users/sudes/OneDrive/Documents/chromedriver-win64/chromedriver.exe"
service = Service(executable_path=path)
driver = webdriver.Chrome(service=service)

driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": """
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    });
    """
})

image_folder = "product_images"
if not os.path.exists(image_folder):
    os.makedirs(image_folder)


def download_image(image_url, product_name):
    try:
        response = requests.get(image_url, stream=True)
        if response.status_code == 200:
            safe_name = "".join([c for c in product_name if c.isalpha() or c.isdigit() or c in " -_"]).rstrip()
            filename = os.path.join(image_folder, f"{safe_name}.jpg")
            with open(filename, "wb") as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            return filename
        return None
    except Exception as e:
        print(f"Error downloading image: {str(e)}")
        return None


try:
    driver.get("https://www.dermstore.com/c/makeup/bestsellers/")

    try:
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "button#onetrust-accept-btn-handler")
        )).click()
        print("Cookies accepted")
    except Exception as e:
        print("Cookie consent not found:", str(e))

    products = []


    def get_product_links():
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "product-card-wrapper[data-e2e^='search_list-item-'] a.product-item"))
        )
        return [product.get_attribute("href") for product in driver.find_elements(
            By.CSS_SELECTOR, "product-card-wrapper[data-e2e^='search_list-item-'] a.product-item"
        )]


    def get_product_details(url):
        driver.get(url)
        try:
            product = {
                'name': WebDriverWait(driver, 15).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "h1#product-title"))
                ).text.strip(),
                'price': WebDriverWait(driver, 15).until(
                    EC.visibility_of_element_located((By.CSS_SELECTOR, "#product-price span"))
                ).text.strip(),
                'rating': "N/A",
                'reviews_count': "N/A",
                'image_url': WebDriverWait(driver, 15).until(
                    EC.visibility_of_element_located(
                        (By.CSS_SELECTOR, "#carousel > figure:nth-child(1) > picture > img"))
                ).get_attribute("src"),
                'size': 'N/A',
                'skinType': 'N/A',
                'ingredients': [],
                'skinConcerns': "N/A"
            }

            try:
                size_element = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[aria-labelledby="Volume"] li'))
                )
                product['size'] = size_element.get_attribute("innerHTML").strip()
            except:
                pass

            try:
                rating_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR,
                                                    "#reviews > div > div > div.lg\\:col-span-3.flex.flex-col > div:nth-child(1) > div > span"))
                )
                product['rating'] = rating_element.text.strip()

                reviews_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR,
                                                    "#reviews > div > div > div.lg\\:col-span-3.flex.flex-col > div:nth-child(1) > p"))
                )
                product['reviews_count'] = reviews_element.text.strip().replace(" reviews", "")
            except:
                print(f"Rating/reviews not found for {product['name']}")

            try:
                concerns_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[aria-labelledby="Skin-Type-&-Concerns"] p'))
                )
                concerns_html = concerns_element.get_attribute('innerHTML')

                ideal_for_these_concerns_section_pattern = r'Ideal for these Concerns:</strong>(.*?)(?=<strong>|$)'
                section_match = re.search(ideal_for_these_concerns_section_pattern, concerns_html, re.DOTALL)

                if section_match:
                    concerns_section = section_match.group(1).strip()
                    concerns_list = re.findall(r'<a[^>]+>([^<]+)</a>', concerns_section)
                    product['skinConcerns'] = ', '.join(concerns_list)

            except Exception as e:
                product['skinConcerns'] = "N/A"

            try:
                skin_type_section_pattern = r'Skin Type:</strong>(.*?)(?=<strong>|$)'
                skin_type_match = re.search(skin_type_section_pattern, concerns_html, re.DOTALL)

                if skin_type_match:
                    skin_type_section = skin_type_match.group(1).strip()
                    skin_type_list = re.findall(r'<a[^>]+>([^<]+)</a>', skin_type_section)
                    product['skinType'] = ', '.join(skin_type_list)

            except Exception as e:
                product['skinType'] = "N/A"

            try:
                ingredients_section_pattern = r'Ingredient:</strong>(.*?)(?=<strong>|$)'
                ingredients_match = re.search(ingredients_section_pattern, concerns_html, re.DOTALL)

                if ingredients_match:
                    ingredients_section = ingredients_match.group(1).strip()
                    ingredients_list = re.findall(r'<a[^>]+>([^<]+)</a>', ingredients_section)
                    product['ingredients'] = ', '.join(ingredients_list)

            except Exception as e:
                product['ingredients'] = "N/A"

            image_path = download_image(product['image_url'], product['name'])
            product['image_path'] = image_path if image_path else "Failed to download"

            return product
        except Exception as e:
            print(f"Error getting details from {url}: {str(e)}")
            return None
        finally:
            driver.back()
            time.sleep(random.uniform(1, 3))

    while len(products) < 1000:
        product_links = get_product_links()
        print(f"Found {len(product_links)} products on this page")

        for link in product_links:
            if len(products) >= 1000:
                break
            if product := get_product_details(link):
                products.append(product)
                print(f"\nCollected {len(products)}:")
                print(f"Product Name: {product['name']}")
                print(f"Price: {product['price']}")
                print(f"Size: {product.get('size', 'N/A')}")
                print(f"Rating: {product['rating']}/5")
                print(f"Review: {product['reviews_count']}")
                print(f"Key Ingredients: {product['ingredients']}")
                print(f"Image URL: {product['image_url']}")
                print(f"Skin Concerns: {product['skinConcerns']}")
                print(f"Skin Types: {product['skinType']}\n")

        try:
            next_btn = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#product-list-page pagination-wrapper .next-page-button"))
            )
            print("Found 'Next' button")

            driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_btn)
            time.sleep(random.uniform(0.5, 1.5))
            driver.execute_script("arguments[0].click();", next_btn)
            print("Clicked 'Next' button")

            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "product-card-wrapper"))
            )
            print("Successfully navigated to next page")
        except Exception as e:
            print("No more pages or error navigating:", str(e))
            break

finally:
    driver.quit()
    print("\n=== SCRAPING RESULTS ===")
    print(f"Total products collected: {len(products)}")

    try:
        df = pd.DataFrame([{
            'Product Name': p['name'],
            'Price': p['price'],
            'Size': p['size'],
            'Rating': p['rating'],
            'Review': p['reviews_count'],
            'Skin Concerns': p['skinConcerns'],
            'Key Ingredients': p['ingredients'],
            'Images': p['image_url'],
            'Skin Types': p['skinType'],
        } for p in products])

        df.to_csv('makeup_products.csv', index=False)
        print("\nSuccessfully saved data to dermstore_products.csv")

    except Exception as e:
        print(f"\nError saving CSV: {str(e)}")


#Design a database and store in sql and hdfs#

import os
from hdfs import InsecureClient

# Set up HDFS client
hdfs_client = InsecureClient('http://100.104.16.66:9870', user='master-node')

# Local CSV file path
local_csv_file = 'C:/Users/sudes/makeup_products.csv'

# HDFS target folder
hdfs_target_folder = '/Data-Engineer-1/spark-cluster/'

# Ensure HDFS folder exists
hdfs_client.makedirs(hdfs_target_folder)

# Target file path in HDFS
hdfs_file_path = os.path.join(hdfs_target_folder, os.path.basename(local_csv_file))

try:
    hdfs_client.upload(hdfs_file_path, local_csv_file)
    print(f'Successfully uploaded {local_csv_file} to {hdfs_file_path}')
except Exception as e:
    print(f'Failed to upload file: {e}')
            
#sql database load
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