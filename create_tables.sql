CREATE TABLE IF NOT EXISTS dim_location (
    location_id INTEGER PRIMARY KEY,
    branch TEXT,
    city TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id INTEGER PRIMARY KEY,
    product_line TEXT
);

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
);

-- Add indexes for performance on foreign keys and commonly queried columns
CREATE INDEX IF NOT EXISTS idx_fact_sales_location_id ON fact_sales (location_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id ON fact_sales (product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON fact_sales (date);  -- For potential date-based queries
CREATE INDEX IF NOT EXISTS idx_dim_location_branch ON dim_location (branch);  -- For grouping in reports
CREATE INDEX IF NOT EXISTS idx_dim_product_product_line ON dim_product (product_line);  -- For grouping in reports