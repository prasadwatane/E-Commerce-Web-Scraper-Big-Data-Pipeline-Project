# E-Commerce-Web-Scraper-Big-Data-Pipeline-Project

## Project Overview
This project was developed as part of a Big Data & Databases course assignment. It automates the extraction and processing of data from three different e-commerce domains:

Dermstore (Moisturizers & Makeup Products)

BooksToScrape (Books)

Steam (Games)

The scraped data is cleaned, transformed, and stored using distributed systems like HDFS and MySQL, enabling analytical queries for decision-making.

## 📌 Key Features
🔍 Web Scraping using Python and Selenium for dynamic content extraction.

📦 Scrapes product details:

Product Name

Price

Size/Volume

Ratings & Review Count

Key Ingredients

Image URLs

Skin Types & Concerns (for Dermstore)

📷 Downloads and stores product images locally.

🕸️ Politeness features like user-agent rotation and random delays.

📁 Stores scraped data as CSV and uploads to HDFS.

🛢️ Inserts data into MySQL with structured schema (Brand, Product, Image, SkinType, Ingredient, etc.).

🧪 Simulated HDFS redundancy test to verify fault-tolerance.

📊 Supports execution of advanced business queries for analytics.

 
 
 ## 🧰 Technologies Used
Python (Selenium, Requests, Pandas)

Docker & Docker Swarm

Hadoop (HDFS)

MySQL

Tailscale (for remote cluster setup)

Jupyter Notebooks (Live demo)

    
## 📊 Business Queries Implemented
📚 Top 10 books with highest availability.

🛍️ Impact of title length on marketing performance.

💸 Analysis of book pricing vs. ratings.

🧴 Products with rare ingredients.

🌟 Top 5 most-reviewed products with skin type.

👵 Aging skin products with high ratings under $40.

📈 Average ratings by skin concern.

🎮 Top 10 most expensive Steam games.

💰 Free vs. paid game comparison.

📅 Trends in game releases and pricing.

🧾 Distribution of games by price category.

## 📥 Prerequisites
Python 3

ChromeDriver & Google Chrome

MySQL Server (Docker or local)

Hadoop Cluster (standalone or Docker-based)

HDFS CLI with alias dev

⚙️ Setup Instructions
Install dependencies:

bash
Copy
Edit
pip install requests pandas selenium webdriver-manager pymysql hdfs
Configure HDFS:

Ensure hdfscli is installed and alias dev is configured.

Update MySQL Config in Script:

Modify mysql_insertion.py with your DB host, user, password, and database.

Run the Scraper:

bash
Copy
Edit
python chetanw.py
Upload to HDFS:

bash
Copy
Edit
python hdfs_upload.py
Insert Data into MySQL:

bash
Copy
Edit
python mysql_insertion.py
📺 Live Demo
📓 Jupyter Notebook Demo

🎥 Demo Video
## Redundancy Test
redundancy test
— by turn off 1 workers while reading and saving data
