"""
E-Commerce Analytics ETL Pipeline
Extracts data from CSV, transforms, loads into PostgreSQL star schema
"""

import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine
import logging
import os
from dotenv import load_dotenv
from typing import Tuple, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

class EcommerceETL:
    """ETL pipeline for e-commerce analytics"""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        self.engine = None
        self.conn = None
        self.df_raw = None
        self.stats = {
            'rows_extracted': 0,
            'rows_after_cleaning': 0,
            'customers_loaded': 0,
            'products_loaded': 0,
            'dates_loaded': 0,
            'transactions_loaded': 0,
            'errors': []
        }
    
    def connect(self):
        """Establish database connections"""
        try:
            # SQLAlchemy engine for pandas
            connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)
            
            # psycopg2 connection for direct SQL
            self.conn = psycopg2.connect(**self.db_config)
            
            logger.info("✓ Database connections established")
            return True
        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            self.stats['errors'].append(f"Connection error: {e}")
            return False
    
    def extract(self, filepath: str) -> bool:
        """Extract data from CSV"""
        try:
            logger.info(f"Extracting data from {filepath}")
            
            # Read CSV with appropriate dtypes
            dtype_dict = {
                'InvoiceNo': str,
                'StockCode': str,
                'Description': str,
                'Quantity': int,
                'InvoiceDate': str,
                'UnitPrice': float,
                'CustomerID': str,
                'Country': str
            }
            
            self.df_raw = pd.read_csv(filepath, dtype=dtype_dict, encoding='utf-8', encoding_errors='ignore')
            self.stats['rows_extracted'] = len(self.df_raw)
            
            logger.info(f"✓ Extracted {len(self.df_raw):,} rows")
            logger.info(f"  Columns: {self.df_raw.columns.tolist()}")
            logger.info(f"  Date range: {self.df_raw['InvoiceDate'].min()} to {self.df_raw['InvoiceDate'].max()}")
            logger.info(f"  Missing values:\n{self.df_raw.isnull().sum()}")
            
            return True
        except Exception as e:
            logger.error(f"✗ Extraction failed: {e}")
            self.stats['errors'].append(f"Extraction error: {e}")
            return False
    
    def transform(self) -> bool:
        """Transform and clean data"""
        try:
            logger.info("Transforming data")
            df = self.df_raw.copy()
            initial_rows = len(df)
            
            # 1. Remove duplicates
            df = df.drop_duplicates()
            logger.info(f"  → Removed {initial_rows - len(df)} duplicate rows")
            
            # 2. Handle missing values
            # Drop rows without CustomerID (can't attribute sales)
            df = df.dropna(subset=['CustomerID'])
            logger.info(f"  → Dropped {initial_rows - len(df)} rows with null CustomerID")
            
            # Drop rows without Description
            df = df.dropna(subset=['Description'])
            
            # 3. Data type conversions
            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
            df['CustomerID'] = df['CustomerID'].astype(int)
            df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
            df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce')
            
            # 4. Data quality filters
            # Remove cancelled orders (InvoiceNo starts with 'C')
            df = df[~df['InvoiceNo'].str.startswith('C', na=False)]
            
            # Remove negative quantities and prices
            df = df[(df['Quantity'] > 0) & (df['UnitPrice'] > 0)]
            logger.info(f"  → Removed negative/zero quantity and price rows")
            
            # Remove outliers (quantity > 10000 likely data errors)
            df = df[df['Quantity'] <= 10000]
            
            # 5. Feature engineering
            df['TotalAmount'] = df['Quantity'] * df['UnitPrice']
            df['Year'] = df['InvoiceDate'].dt.year
            df['Month'] = df['InvoiceDate'].dt.month
            df['Day'] = df['InvoiceDate'].dt.day
            df['DayOfWeek'] = df['InvoiceDate'].dt.dayofweek
            df['Hour'] = df['InvoiceDate'].dt.hour
            
            # 6. Text cleaning
            df['Description'] = df['Description'].str.strip().str.upper()
            df['Country'] = df['Country'].str.strip()
            
            self.df_clean = df
            self.stats['rows_after_cleaning'] = len(df)
            
            logger.info(f"✓ Transformation complete: {initial_rows:,} → {len(df):,} rows ({len(df)/initial_rows*100:.1f}% retained)")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Transformation failed: {e}")
            self.stats['errors'].append(f"Transformation error: {e}")
            return False
    
    def load_dimensions(self) -> bool:
        """Load dimension tables"""
        try:
            logger.info("Loading dimension tables")
            cursor = self.conn.cursor()
            
            # 1. Load dim_dates
            logger.info("  → Loading dim_dates")
            dates_df = pd.DataFrame()
            dates_df['full_date'] = pd.to_datetime(self.df_clean['InvoiceDate'].dt.date.unique())
            dates_df['year'] = dates_df['full_date'].dt.year
            dates_df['quarter'] = dates_df['full_date'].dt.quarter
            dates_df['month'] = dates_df['full_date'].dt.month
            dates_df['month_name'] = dates_df['full_date'].dt.strftime('%B')
            dates_df['week'] = dates_df['full_date'].dt.isocalendar().week
            dates_df['day_of_month'] = dates_df['full_date'].dt.day
            dates_df['day_of_week'] = dates_df['full_date'].dt.dayofweek
            dates_df['day_name'] = dates_df['full_date'].dt.strftime('%A')
            dates_df['is_weekend'] = dates_df['day_of_week'].isin([5, 6])
            
            # Insert dates
            for _, row in dates_df.iterrows():
                cursor.execute("""
                    INSERT INTO dim_dates (full_date, year, quarter, month, month_name, 
                                          week, day_of_month, day_of_week, day_name, is_weekend)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (full_date) DO NOTHING
                """, tuple(row))
            
            self.conn.commit()
            self.stats['dates_loaded'] = len(dates_df)
            logger.info(f"    ✓ Loaded {len(dates_df)} dates")
            
            # 2. Load dim_customers
            logger.info("  → Loading dim_customers")
            customers_df = self.df_clean.groupby('CustomerID').agg({
                'Country': 'first',
                'InvoiceDate': ['min', 'max'],
                'InvoiceNo': 'nunique',
                'TotalAmount': 'sum'
            }).reset_index()
            
            customers_df.columns = ['customer_id', 'country', 'first_purchase_date', 
                                   'last_purchase_date', 'total_orders', 'lifetime_value']
            
            for _, row in customers_df.iterrows():
                cursor.execute("""
                    INSERT INTO dim_customers (customer_id, country, first_purchase_date, 
                                              last_purchase_date, total_orders, lifetime_value)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO UPDATE SET
                        last_purchase_date = EXCLUDED.last_purchase_date,
                        total_orders = EXCLUDED.total_orders,
                        lifetime_value = EXCLUDED.lifetime_value,
                        updated_at = CURRENT_TIMESTAMP
                """, tuple(row))
            
            self.conn.commit()
            self.stats['customers_loaded'] = len(customers_df)
            logger.info(f"    ✓ Loaded {len(customers_df)} customers")
            
            # 3. Load dim_products
            logger.info("  → Loading dim_products")
            products_df = self.df_clean.groupby('StockCode').agg({
                'Description': 'first',
                'UnitPrice': 'mean'  # Average price if it varies
            }).reset_index()
            
            for _, row in products_df.iterrows():
                cursor.execute("""
                    INSERT INTO dim_products (stock_code, description, unit_price)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (stock_code) DO UPDATE SET
                        description = EXCLUDED.description,
                        unit_price = EXCLUDED.unit_price
                """, tuple(row))
            
            self.conn.commit()
            self.stats['products_loaded'] = len(products_df)
            logger.info(f"    ✓ Loaded {len(products_df)} products")
            
            cursor.close()
            logger.info("✓ All dimensions loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"✗ Dimension loading failed: {e}")
            self.conn.rollback()
            self.stats['errors'].append(f"Dimension load error: {e}")
            return False
    
    def load_facts(self) -> bool:
        """Load fact table"""
        try:
            logger.info("Loading fact_transactions")
            cursor = self.conn.cursor()
            
            # Prepare data with foreign keys
            transactions = []
            
            for _, row in self.df_clean.iterrows():
                # Get foreign keys
                cursor.execute("SELECT customer_id FROM dim_customers WHERE customer_id = %s", 
                             (int(row['CustomerID']),))
                customer_result = cursor.fetchone()
                
                cursor.execute("SELECT product_id FROM dim_products WHERE stock_code = %s", 
                             (row['StockCode'],))
                product_result = cursor.fetchone()
                
                cursor.execute("SELECT date_id FROM dim_dates WHERE full_date = %s", 
                             (row['InvoiceDate'].date(),))
                date_result = cursor.fetchone()
                
                if customer_result and product_result and date_result:
                    transactions.append((
                        row['InvoiceNo'],
                        customer_result[0],
                        product_result[0],
                        date_result[0],
                        row['InvoiceDate'],
                        int(row['Quantity']),
                        float(row['UnitPrice'])
                    ))
            
            # Batch insert for performance
            insert_query = """
                INSERT INTO fact_transactions 
                (invoice_no, customer_id, product_id, date_id, invoice_date, quantity, unit_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            execute_batch(cursor, insert_query, transactions, page_size=1000)
            self.conn.commit()
            
            self.stats['transactions_loaded'] = len(transactions)
            logger.info(f"✓ Loaded {len(transactions):,} transactions")
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"✗ Fact loading failed: {e}")
            self.conn.rollback()
            self.stats['errors'].append(f"Fact load error: {e}")
            return False
    
    def refresh_aggregates(self) -> bool:
        """Refresh materialized views"""
        try:
            logger.info("Refreshing materialized views")
            cursor = self.conn.cursor()
            cursor.execute("REFRESH MATERIALIZED VIEW mv_monthly_metrics")
            self.conn.commit()
            cursor.close()
            logger.info("✓ Materialized views refreshed")
            return True
        except Exception as e:
            logger.error(f"✗ Refresh failed: {e}")
            return False
    
    def validate(self) -> Dict:
        """Validate data quality post-load"""
        try:
            logger.info("Validating data")
            cursor = self.conn.cursor()
            
            validation_results = {}
            
            # Check row counts
            cursor.execute("SELECT COUNT(*) FROM fact_transactions")
            validation_results['transaction_count'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dim_customers")
            validation_results['customer_count'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dim_products")
            validation_results['product_count'] = cursor.fetchone()[0]
            
            # Check for data quality issues
            cursor.execute("SELECT COUNT(*) FROM fact_transactions WHERE total_amount < 0")
            validation_results['negative_amounts'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(total_amount) FROM fact_transactions")
            validation_results['total_revenue'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT MIN(invoice_date), MAX(invoice_date) FROM fact_transactions")
            date_range = cursor.fetchone()
            validation_results['date_range'] = f"{date_range[0]} to {date_range[1]}"
            
            cursor.close()
            
            # Log results
            logger.info("="*60)
            logger.info("DATA VALIDATION RESULTS")
            logger.info("="*60)
            for key, value in validation_results.items():
                logger.info(f"  {key}: {value}")
            logger.info("="*60)
            
            return validation_results
            
        except Exception as e:
            logger.error(f"✗ Validation failed: {e}")
            return {}
    
    def close(self):
        """Close database connections"""
        if self.conn:
            self.conn.close()
        if self.engine:
            self.engine.dispose()
        logger.info("✓ Database connections closed")
    
    def run(self, filepath: str) -> bool:
        """Execute full ETL pipeline"""
        logger.info("="*60)
        logger.info("E-COMMERCE ETL PIPELINE STARTED")
        logger.info("="*60)
        start_time = datetime.now()
        
        try:
            # Pipeline steps
            if not self.connect():
                return False
            
            if not self.extract(filepath):
                return False
            
            if not self.transform():
                return False
            
            if not self.load_dimensions():
                return False
            
            if not self.load_facts():
                return False
            
            if not self.refresh_aggregates():
                return False
            
            # Validate
            validation_results = self.validate()
            
            # Success summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("="*60)
            logger.info("✓ ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Rows extracted: {self.stats['rows_extracted']:,}")
            logger.info(f"Rows loaded: {self.stats['transactions_loaded']:,}")
            logger.info(f"Data quality: {(self.stats['rows_after_cleaning']/self.stats['rows_extracted']*100):.1f}% retained")
            logger.info("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Pipeline failed: {e}")
            return False
        
        finally:
            self.close()

if __name__ == "__main__":
    # Run pipeline
    pipeline = EcommerceETL()
    success = pipeline.run('data/raw/ecommerce.csv')
    
    if success:
        print("\n✓ Pipeline completed successfully!")
    else:
        print("\n✗ Pipeline failed. Check logs for details.")