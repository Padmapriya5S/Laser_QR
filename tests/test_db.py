import sys
import os
import psycopg2

# Add the project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.db_config import DB_CONFIG

def test_database_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT serials.serial, batches.vendor, tms_data.fault 
            FROM serials 
            JOIN batches ON serials.batch_id = batches.batch_id 
            JOIN tms_data ON serials.serial = tms_data.serial
        """)
        results = cursor.fetchall()
        print("Test passed! Data retrieved:", results)
        return True
    except psycopg2.Error as e:
        print(f"Test failed: {e}")
        return False
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    test_database_connection()