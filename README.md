# Assessment
Data Processing Pipeline - README
Overview
This project implements a data processing pipeline that:

Downloads a compressed CSV file from a remote URL

1. Performs data transformation and cleaning
2. Stores the processed data in a SQL database
3. Includes comprehensive error handling and logging
4. Optimizes performance through chunked processing


Python 3.8+ :
Required packages (install with pip install -r requirements.txt):
pandas, sqlalchemy, requests, numpy

Database Setup:
Create the database schema by running:

bash
psql -U your_username -d your_database -f schema.sql
Update the database configuration in the Python script:

python
DB_CONFIG = {
    'dialect': 'postgresql',
    'username': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432',
    'database': 'your_database'
}

Environment Variables (optional):
You can set database credentials as environment variables:

bash
export DB_USER=your_username
export DB_PASSWORD=your_password
export DB_NAME=your_database

How to Run
Execute the main script:

bash
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
