import csv
import random
from datetime import datetime, timedelta
import psycopg2
import os
import sys

# Add project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.db_config import DB_CONFIG

def generate_batch_data(num_entries=20):
    headers = ["batch_id", "vendor", "supply_date", "warranty_end", "material_type", "quantity"]
    data = []
    vendors = ["Vendor A", "Vendor B", "Vendor C"]
    materials = ["Steel Clip", "Rubber Pad", "Composite"]
    for i in range(num_entries):
        batch_id = f"Lot #{i+100:03d}"
        vendor = random.choice(vendors)
        supply_date = (datetime.now() - timedelta(days=random.randint(180, 360))).strftime("%Y-%m-%d")
        warranty_end = (datetime.strptime(supply_date, "%Y-%m-%d") + timedelta(days=random.randint(365, 730))).strftime("%Y-%m-%d")
        material_type = random.choice(materials)
        quantity = random.randint(8000, 12000)
        data.append([batch_id, vendor, supply_date, warranty_end, material_type, quantity])
    return data, headers

def generate_serials_data(batches):
    headers = ["serial", "batch_id"]
    data = []
    for batch in batches:
        for i in range(random.randint(5, 15)):
            serial = f"{batch[0].replace(' ', '')}-{str(i+1).zfill(6)}"  # batch[0] is batch_id
            data.append([serial, batch[0]])
    return data, headers

def generate_tms_data(serials):
    headers = ["serial", "track_id", "install_date", "last_inspection_date", "fault", "usage_days"]
    data = []
    start_date = datetime(2025, 1, 1)
    for serial in serials:
        install_date = start_date + timedelta(days=random.randint(0, 180))
        usage_days = (datetime(2025, 9, 18) - install_date).days
        last_inspection = install_date + timedelta(days=random.randint(90, 270))
        fault = random.choices(["Wear", "Crack", "None"], weights=[15, 15, 70])[0]
        track_id = f"Track {chr(88 + random.randint(0, 2))}"
        data.append([serial[0], track_id, install_date.strftime("%Y-%m-%d"), last_inspection.strftime("%Y-%m-%d"), fault, usage_days])
    return data, headers

def save_to_csv(data, filename, headers):
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)  # Write headers as a row
        writer.writerows(data)    # Write data rows

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Generate and save data
    base_dir = os.path.join(project_root, 'data', 'mock')
    os.makedirs(base_dir, exist_ok=True)

    batches_data, batches_headers = generate_batch_data()
    save_to_csv(batches_data, os.path.join(base_dir, 'batches.csv'), batches_headers)
    serials_data, serials_headers = generate_serials_data(batches_data)
    save_to_csv(serials_data, os.path.join(base_dir, 'serials.csv'), serials_headers)
    tms_data, tms_headers = generate_tms_data(serials_data)
    save_to_csv(tms_data, os.path.join(base_dir, 'tms_data.csv'), tms_headers)

    # Import to PostgreSQL (skip header handled by copy_from)
    def import_csv_without_header(cursor, filename, table, columns):
        with open(filename, 'r') as f:
            next(f)  # Skip header
            cursor.copy_from(f, table, sep=',', columns=columns)

    import_csv_without_header(cursor, os.path.join(base_dir, 'batches.csv'), 'batches', ('batch_id', 'vendor', 'supply_date', 'warranty_end', 'material_type', 'quantity'))
    import_csv_without_header(cursor, os.path.join(base_dir, 'serials.csv'), 'serials', ('serial', 'batch_id'))
    import_csv_without_header(cursor, os.path.join(base_dir, 'tms_data.csv'), 'tms_data', ('serial', 'track_id', 'install_date', 'last_inspection_date', 'fault', 'usage_days'))

    conn.commit()
    print("Mock data generated and imported successfully!")

except psycopg2.Error as e:
    print(f"Error importing data: {e}")
except Exception as e:
    print(f"Error: {e}")
finally:
    if conn:
        conn.close()
        print("Database connection closed.")