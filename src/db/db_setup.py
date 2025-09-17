import sys
import os
import psycopg2

# Add the project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.db_config import DB_CONFIG

def create_database():
    # Connect to default postgres DB
    conn = psycopg2.connect(
        dbname="postgres",
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Check if database exists
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'railway_optimized'")
    exists = cursor.fetchone()
    
    if not exists:
        cursor.execute("CREATE DATABASE railway_optimized;")
        print("Database 'railway_optimized' created.")
    else:
        print("Database 'railway_optimized' already exists.")
    
    conn.close()

try:
    # Create the database
    create_database()

    # Connect to the new/existing database
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Create tables
    cursor.execute('''CREATE TABLE IF NOT EXISTS batches (
        batch_id TEXT PRIMARY KEY,
        vendor TEXT,
        supply_date DATE,
        warranty_end DATE,
        material_type TEXT,
        quantity INTEGER
    );''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS serials (
        serial TEXT PRIMARY KEY,
        batch_id TEXT,
        FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
    );''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS tms_data (
        serial TEXT PRIMARY KEY,
        track_id TEXT,
        install_date DATE,
        last_inspection_date DATE,
        fault TEXT,
        usage_days INTEGER,
        FOREIGN KEY (serial) REFERENCES serials(serial)
    );''')

    conn.commit()
    print("Tables created successfully!")

except psycopg2.Error as e:
    print(f"Error setting up database: {e}")
finally:
    if conn:
        conn.close()
        print("Database connection closed.")