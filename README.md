# shein_bigdata
This is a ETL for https://www.kaggle.com/datasets/oleksiimartusiuk/e-commerce-data-shein .
User Manual: ETL Pipeline with Python, PySpark, PostgreSQL & Superset
Overview

This ETL pipeline extracts Shein product data from CSV files, transforms it using PySpark, loads it into a PostgreSQL database, and allows you to visualize insights using Apache Superset.

Pipeline Stages:

Extract – Read raw CSV files

Transform – Clean, standardize, and prepare data for analysis

Load – Save transformed data into PostgreSQL

Visualize – Use Superset to create charts and dashboards

System Requirements

OS: Linux / Windows / macOS

Python: ≥ 3.9

PySpark: ≥ 3.4

PostgreSQL: ≥ 12

Superset: Latest version recommended

RAM: ≥ 8GB (for large datasets)

Step 1: Download the Code

Open a terminal or command prompt

Clone the repository:

git clone <YOUR_GITHUB_REPO_URL> ETL
cd ETL/etl


You should see the following folder structure:

etl/
 ├── extract/
 ├── transform/
 ├── load/
 ├── ultility/
 └── data/ (create this if not present)

Step 2: Set up Python Environment

Create a virtual environment:

python3 -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows


Install required packages:

pip install -r requirements.txt


(requirements.txt should contain pyspark, psycopg2-binary, etc.)

Step 3: Prepare Data

Create folders for raw and processed data:

mkdir -p data/raw
mkdir -p data/processed


Place all Shein CSV files into data/raw

Step 4: Run Extract (Optional)

If your data source requires an extract step:

python3 extract/execute.py data/raw


This reads raw files from your source into data/raw.

Logs are saved in extract.log

Step 5: Run Transform

Execute the transformation:

python3 transform/execute.py data/raw data/processed


What happens:

Reads all CSVs in data/raw

Cleans and standardizes columns

Generates:

stage1/products (cleaned products)

stage2/master_table (combined master table)

stage3/ (query-optimized tables)

Logs are saved in transform.log

Step 6: Run Load

Ensure PostgreSQL is running

Create a database if not existing:

sudo -u postgres psql
CREATE DATABASE shein_etl;
\q


Run the load step:

python3 load/execute.py data/processed postgres <your_password>


What happens:

Loads all parquet tables into PostgreSQL

Tables include:

master_table

products_metadata

category_stats

Step 7: Connect Superset to PostgreSQL

Start Superset:

superset db upgrade
superset fab create-admin
superset init
superset run -p 8088 --with-threads --reload --debugger


Login to Superset (http://localhost:8088)

Add a database connection:

Go to Data → Databases → + Database

Connection string:

postgresql+psycopg2://postgres:<your_password>@localhost:5432/shein_etl


Test the connection and save

Step 8: Create Dataset in Superset

Go to Data → Datasets → + Dataset

Choose your database (shein_etl) and schema (public)

Select table: category_stats

Save the dataset

Step 9: Build First Chart: Products per Category

Go to Charts → + Chart

Choose dataset: category_stats

Choose chart type: Bar Chart

Configure:

Metrics: product_count → Sum

Group By: category

Click Run → preview chart

Save chart as: Products per Category

Optionally add to a dashboard

Step 10: Build Additional Charts

You can create other charts like:

Average price per category

Total discount per category

Top-selling products

Tip: Use stage3/ tables (products_metadata, category_stats) for faster queries.

Step 11: Dashboard

Go to Dashboards → + Dashboard

Add charts to the dashboard for a complete visual overview

Step 12: Troubleshooting

Transform fails: Ensure data/raw exists and contains CSVs

Load fails: Make sure PostgreSQL is running and credentials are correct

Superset cannot connect: Verify the connection string:

postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>


Empty tables in PostgreSQL: Check that transform stage created parquet files

Conclusion

You now have a fully working ETL pipeline:

Extract raw CSV data

Transform and clean it with PySpark

Load it into PostgreSQL

Visualize insights in Superset

You can extend it by adding new charts, dashboards, or additional ETL sources.
