import os
import gzip
import io
import logging
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, exc
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mysql_data_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# MySQL Configuration
DB_CONFIG = {
    'dialect': 'mysql',
    'driver': 'pymysql',
    'username': 'your_username',
    'password': 'your_password',
    'host': 'localhost',
    'port': '3306',
    'database': 'product_data',
    'charset': 'utf8mb4'  # For proper Unicode support
}
CHUNKSIZE = 50000  # Optimal chunk size for MySQL

def get_db_connection_string():
    """Generate MySQL connection string"""
    return (
        f"{DB_CONFIG['dialect']}+{DB_CONFIG['driver']}://"
        f"{DB_CONFIG['username']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
        f"{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
    )

def download_and_extract_data(url: str, file_path: str) -> bool:
    """Download and extract gzipped CSV file"""
    try:
        logger.info(f"Downloading dataset from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
            logger.info("Extracting CSV data from gzip")
            chunks = pd.read_csv(gz_file, chunksize=CHUNKSIZE, low_memory=False)
            
            # Write first chunk to file
            first_chunk = True
            for chunk in chunks:
                chunk.to_csv(file_path, mode='w' if first_chunk else 'a', 
                           header=first_chunk, index=False)
                first_chunk = False
                
        logger.info(f"Data successfully saved to {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error downloading/extracting data: {str(e)}")
        return False

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Perform MySQL-compatible data transformations"""
    try:
        logger.info("Starting data transformation for MySQL")
        
        # Convert numeric columns (MySQL compatible)
        df['sku_id'] = pd.to_numeric(df['sku_id'], errors='coerce').astype('Int64')
        df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce').astype('Int64')

        # Drop colums of img and URL (optional if they are required you can skip deleting it)
        df=df.drop(['product_big_img'],axis=1)
        df=df.drop(['product_big_img'],axis=1)
        df=df.drop(['product_big_img'],axis=1)
        df=df.drop(['product_big_img'],axis=1)
        
        # Drop rows with null sku_id
        initial_count = len(df)
        df.dropna(subset=['sku_id'], inplace=True)
        logger.info(f"Dropped {initial_count - len(df)} rows with null sku_id")
        
        # MySQL-friendly string columns (using VARCHAR equivalent)
        str_cols = [
            'availability', 'venture_category2_name_en', 'venture_category1_name_en',
            'brand_name', 'venture_category_name_local', 'seller_name', 'business_area',
            'business_type', 'product_name', 'product_url', 'description'
        ]
        df[str_cols] = df[str_cols].astype('string')
        
        # MySQL numeric conversions
        num_cols = {
            'platform_commission_rate': (0, 2),
            'product_commission_rate': (None, 2),
            'current_price': (None, 2),
            'price': (None, 2),
            'rating_avg_value': (None, 1),
            'seller_rating': (lambda x: x/20, 1),
            'bonus_commission_rate': (lambda x: x/100, 1),
            'discount_percentage': (lambda x: x/100, 2)
        }
        
        for col, (transform, decimals) in num_cols.items():
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if transform is not None:
                if callable(transform):
                    df[col] = transform(df[col])
                else:  # fill value
                    df[col].fillna(transform, inplace=True)
            if decimals is not None:
                df[col] = df[col].round(decimals)
        
        # MySQL boolean conversion
        df['is_free_shipping'] = (
            df['is_free_shipping'].astype('string')
            .str.lower()
            .map({'true': True, 'false': False, 'yes': True, 'no': False, '1': True, '0': False})
            .astype('boolean')
        )
        
        # Handle descriptions (MySQL TEXT field)
        df['description'].fillna('No Description', inplace=True)
        
        # Check for duplicates
        if df['sku_id'].duplicated().any():
            logger.warning(f"Found {df['sku_id'].duplicated().sum()} duplicate sku_ids")
            df.drop_duplicates(subset=['sku_id'], keep='last', inplace=True)
        
        logger.info("Data transformation completed")
        return df
        
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise

def save_to_mysql(df: pd.DataFrame, table_name: str = 'products'):
    """Save DataFrame to MySQL database with optimizations"""
    try:
        logger.info(f"Connecting to MySQL and saving data to {table_name}")
        
        engine = create_engine(
            get_db_connection_string(),
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        
        # MySQL-specific optimizations
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',  # Change to 'replace' for full refresh
            index=False,
            chunksize=CHUNKSIZE,
            method='multi',  # Faster bulk insert
            dtype={
                'description': 'TEXT',
                'product_url': 'TEXT',
                'venture_category_name_local': 'VARCHAR(255) CHARACTER SET utf8mb4'
            }
        )
        
        logger.info(f"Successfully saved {len(df)} records to MySQL")
        
    except exc.IntegrityError as e:
        logger.warning(f"Duplicate entry found: {str(e)}")
        # Implement your duplicate handling strategy here
    except exc.OperationalError as e:
        logger.error(f"MySQL connection error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error saving to MySQL: {str(e)}")
        raise
    finally:
        if 'engine' in locals():
            engine.dispose()

def main():
    try:
        url = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
        csv_file_path = 'mysql_products_data.csv'
        
        # Download data if not exists
        if not os.path.exists(csv_file_path):
            if not download_and_extract_data(url, csv_file_path):
                raise Exception("Failed to download/extract data")
        
        # Process data in chunks for MySQL
        for chunk in pd.read_csv(csv_file_path, chunksize=CHUNKSIZE, low_memory=False):
            logger.info(f"Processing chunk of {len(chunk)} records")
            transformed_chunk = transform_data(chunk)
            save_to_mysql(transformed_chunk)
            
            # Clear memory
            del transformed_chunk
            
        logger.info("MySQL data processing pipeline completed successfully")
        
    except Exception as e:
        logger.critical(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    start_time = datetime.now()
    logger.info(f"Starting MySQL data processing pipeline at {start_time}")
    
    try:
        main()
    except Exception:
        logger.error("Pipeline terminated with errors")
    finally:
        duration = datetime.now() - start_time
        logger.info(f"Pipeline completed in {duration.total_seconds():.2f} seconds")
