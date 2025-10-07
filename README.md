# Supermarket Sales ETL Pipeline

This project implements a simple ETL (Extract, Transform, Load) pipeline for the [Supermarket Sales dataset](https://www.kaggle.com/datasets/lovishbansal123/sales-of-a-supermarket) using Ralph Kimball's dimensional modeling (star schema). It:

- Downloads the dataset from Kaggle.
- Creates dimension tables (`dim_product`, `dim_location`) and a fact table (`fact_sales`).
- Loads data into a SQLite database (`supermarket.db`) with primary keys, foreign keys, and performance indexes.
- Runs sample window function queries (e.g., ranking, cumulative sums, LAG for day-over-day changes).

The schema is lightweight: surrogate keys for dimensions, measures like `total` and `gross_income` in facts, and SQL window functions for analysis.

## Prerequisites

- Python 3.10+ (tested on 3.11).
- Kaggle account and API token:
  1. Log in to [Kaggle](https://www.kaggle.com).
  2. Go to **Account > Settings > API > Create New Token** → Download `kaggle.json`.
  3. Place it in `~/.kaggle/kaggle.json` (create folder if needed).
  4. Run `chmod 600 ~/.kaggle/kaggle.json` (macOS/Linux) for security.

## Setup

1. Clone or download this repo.
2. Create and activate a virtual environment:

   ```bash
   python -m venv venv

   pip install -r requirements.txt
   ```

   ```bash
   # macOS/Linux:
   source venv/bin/activate

   # Windows:
   venv\Scripts\activate
   ```


## Run the Pipeline 

1. Clone or download this repo.
2. Create and activate a virtual environment:

   ```bash
   python pipeline.py
   ```

3. Check contents of SQLlite db

   ```bash
   # Check the fact table
   sqlite3 supermarket.db "SELECT * FROM fact_sales LIMIT 5;"
   ```
   

4. Check the dim_location table
   ```bash
   sqlite3 supermarket.db "SELECT * FROM dim_location LIMIT 5;"
   ```

5. Check the dim_product table
   ```bash
   sqlite3 supermarket.db "SELECT * FROM dim_product LIMIT 5;"
   ```


6. Run the Window functions
   ```bash
   sqlite3 supermarket.db < product_line_sales_ranking_per_branch.sql

   sqlite3 supermarket.db < product_line_sales_percentage_per_branch.sql
  