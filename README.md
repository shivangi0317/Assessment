# Assessment

Overview: This project implements a data processing pipeline that:

1. Downloads a compressed CSV file from a remote URL
2. Performs data transformation and cleaning
3. Stores the processed data in a SQL database
4. Includes comprehensive error handling and logging
5. Optimizes performance through chunked processing


Python 3.8+ :
    Required packages (install with pip install -r requirements.txt):  pandas, sqlalchemy, requests, numpy
    Commmand: pip install pandas    ( similarly for others sqlalchemy, pymysql, requests)

Database Setup:

   Create MySQL Database:

   CREATE DATABASE product_data;
   USE product_data;

Update the database configuration in the Python script:

DB_CONFIG = {
    'dialect': 'mysql',
    'driver': 'pymysql',
    'username': 'pipeline_user',  # Replace with your credentials
    'password': 'secure_password',
    'host': 'localhost',
    'port': '3306',
    'database': 'product_data',
    'charset': 'utf8mb4'
}
CHUNKSIZE = 50000  # Rows processed per batch

Environment Variables (optional):
You can set database credentials as environment variables:

export DB_USER='pipeline_user'
export DB_PASSWORD='secure_password'
export DB_NAME='product_data'

How to Run
Execute the main script:
python script.py


Tasks done by script.py file:

1. Download the data if not already present
2. Process the data in chunks
3. Log all operations to data_processing.log
4. Store results in the database
5. Data Transformations
6. 
The pipeline performs these key transformations:
1. Type conversions (numeric, string, boolean)
2. Null value handling
3. Data validation
4. Deduplication
5. Field-specific transformations (commission rates, ratings, etc.)

Database Schema
The processed data is stored in a products table with this structure:

sql
CREATE TABLE products (
    sku_id BIGINT PRIMARY KEY,
    product_id BIGINT,
    availability VARCHAR(50),
    venture_category2_name_en VARCHAR(255),
    -- ... (other columns)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

Performance Considerations: 
1. Processes data in chunks (configurable via CHUNKSIZE)
2. Uses streaming download for the source file
3. Includes database performance indexes
4. Clears memory between chunks
5. Error Handling

The pipeline includes:
1. Comprehensive logging
2. Graceful failure handling
3. Try-catch blocks for all major operations
4. Validation checks

After successful run, you can query the database:

Example: sql
SELECT sku_id, product_name, current_price 
FROM products 
WHERE is_free_shipping = TRUE 
LIMIT 10;
