import psycopg2
from dotenv import load_dotenv
import os
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self):
        self.db_config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME')
        }
        self.conn = psycopg2.connect(**self.db_config)
        self.cursor = self.conn.cursor()
    
    def validate_all(self):
        """Run all validation checks"""
        print("\n" + "="*60)
        print("DATA VALIDATION REPORT")
        print("="*60)
        
        # Check record counts
        self.cursor.execute("SELECT COUNT(*) FROM transactions")
        tx_count = self.cursor.fetchone()[0]
        print(f"\n✓ Total transactions: {tx_count:,}")
        
        self.cursor.execute("SELECT COUNT(*) FROM customers")
        cust_count = self.cursor.fetchone()[0]
        print(f"✓ Total customers: {cust_count:,}")
        
        self.cursor.execute("SELECT COUNT(*) FROM products")
        prod_count = self.cursor.fetchone()[0]
        print(f"✓ Total products: {prod_count:,}")
        
        # Check data quality
        self.cursor.execute("SELECT COUNT(*) FROM transactions WHERE total_amount < 0")
        negative = self.cursor.fetchone()[0]
        print(f"\n✓ Records with negative amounts: {negative}")
        
        # Check date range
        self.cursor.execute("SELECT MIN(transaction_date), MAX(transaction_date) FROM transactions")
        min_date, max_date = self.cursor.fetchone()
        print(f"✓ Date range: {min_date} to {max_date}")
        
        # Revenue summary
        self.cursor.execute("SELECT SUM(total_amount) FROM transactions")
        total_revenue = self.cursor.fetchone()[0]
        print(f"\n✓ Total revenue: ${total_revenue:,.2f}")
        
        print("\n" + "="*60)
        self.conn.close()

if __name__ == "__main__":
    validator = DataValidator()
    validator.validate_all()