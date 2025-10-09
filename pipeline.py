import os
import logging
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

# Setup logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('Starting data pipeline process.')

try:
    # Step 1: Extract data using Kaggle Python API
    logging.info('Initializing Kaggle API.')
    api = KaggleApi()
    api.authenticate()  # Assumes ~/.kaggle/kaggle.json is set up with credentials
    logging.info('Downloading dataset from Kaggle.')
    api.dataset_download_files('lovishbansal123/sales-of-a-supermarket', path='data', unzip=True)
    logging.info('Dataset downloaded and unzipped.')

    # Load the CSV (standard file name from the dataset)
    csv_path = 'data/supermarket_sales.csv'
    df = pd.read_csv(csv_path)
    logging.info('Data loaded into DataFrame.')

    # Standardize column names: lowercase, replace spaces and special chars with _
    df.columns = [col.lower().replace(' ', '_').replace('%', '') for col in df.columns]

    # Basic cleaning: Convert 'date' to datetime
    df['date'] = pd.to_datetime(df['date'])
<<<<<<< HEAD
    df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce').fillna(1).astype(int)
    df['Unit price'] = pd.to_numeric(df['Unit price'], errors='coerce').fillna(0)
    df['Tax 5%'] = pd.to_numeric(df['Tax 5%'], errors='coerce').fillna(0)
    df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)
    df.dropna(subset=['Invoice ID', 'Product line'], inplace=True)  # Basic quality
<<<<<<< HEAD
=======

>>>>>>> 48d7cca (Additional DQ steps)
=======
    df.dropna(subset=['invoice_id', 'product_line'], inplace=True)  # Basic quality
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(1).astype(int)
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce').fillna(0)
    df['tax_5'] = pd.to_numeric(df['tax_5'], errors='coerce').fillna(0)
    df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0)
>>>>>>> 2898dab (Fix on DQ in pipeline)

    # Step 2: Transform - Create dimension tables
    logging.info('Creating dimension tables.')
    # dim_location: unique branch and city
    dim_location_df = df[['branch', 'city']].drop_duplicates().reset_index(drop=True)
    dim_location_df['location_id'] = dim_location_df.index + 1

    # dim_product: unique product_line
    dim_product_df = df[['product_line']].drop_duplicates().reset_index(drop=True)
    dim_product_df['product_id'] = dim_product_df.index + 1

    # Fact table: Merge IDs and drop original dimension columns
    fact_df = df.copy()
    fact_df = fact_df.merge(dim_location_df, on=['branch', 'city'])
    fact_df = fact_df.merge(dim_product_df, on='product_line')
    fact_df.drop(columns=['branch', 'city', 'product_line'], inplace=True)

    logging.info('Data transformation complete.')

    # Step 3: Load to SQLite
    logging.info('Connecting to SQLite database.')
    conn = sqlite3.connect('supermarket.db')

    # Create tables with lowercase column names
    create_dim_location = """
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id INTEGER PRIMARY KEY,
        branch TEXT,
        city TEXT
    )
    """
    create_dim_product = """
    CREATE TABLE IF NOT EXISTS dim_product (
        product_id INTEGER PRIMARY KEY,
        product_line TEXT
    )
    """
    create_fact_sales = """
    CREATE TABLE IF NOT EXISTS fact_sales (
        invoice_id TEXT PRIMARY KEY,
        location_id INTEGER,
        product_id INTEGER,
        customer_type TEXT,
        gender TEXT,
        unit_price REAL,
        quantity INTEGER,
        tax_5 REAL,
        total REAL,
        date TEXT,
        time TEXT,
        payment TEXT,
        cogs REAL,
        gross_margin_percentage REAL,
        gross_income REAL,
        rating REAL,
        FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
        FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
    )
    """
    conn.execute(create_dim_location)
    conn.execute(create_dim_product)
    conn.execute(create_fact_sales)
    logging.info('Database tables created.')

    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_sales_location_id ON fact_sales (location_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id ON fact_sales (product_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales (date);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dim_location_branch ON dim_location (branch);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dim_product_product_line ON dim_product (product_line);")
    logging.info('Database indexes created.')

    # Load data
    dim_location_df.to_sql('dim_location', conn, if_exists='replace', index=False)
    dim_product_df.to_sql('dim_product', conn, if_exists='replace', index=False)
    fact_df.to_sql('fact_sales', conn, if_exists='replace', index=False)
    logging.info('Data loaded into SQLite database.')

    conn.close()
    logging.info('Pipeline process completed successfully.')

except Exception as e:
    logging.error(f'Error occurred: {str(e)}')
