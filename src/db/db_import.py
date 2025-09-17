import sys
import os
import csv
import random
from datetime import datetime, timedelta
import psycopg2

# Add the project root to sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.db_config import DB_CONFIG

try:
    # Connect to the database
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Define mock data paths
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    mock_dir = os.path.join(base_dir, 'data', 'mock')
    os.makedirs(mock_dir, exist_ok=True)  # Ensure mock directory exists

    # Generate mock UDM batches
    batches = [
        {"batch_id": "Lot #123", "vendor": "Vendor Y", "supply_date": "2025-01-01", "warranty_end": "2026-01-07", "material_type": "Steel Clip", "quantity": 10000},
        {"batch_id": "Lot #124", "vendor": "Vendor Z", "supply_date": "2025-02-01", "warranty_end": "2027-02-01", "material_type": "Rubber Pad", "quantity": 8500}
    ]
    with open(os.path.join(mock_dir, 'batches.csv'), 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=batches[0].keys())
        writer.writeheader()  # Keep header for inspection
        writer.writerows(batches)

    # Generate mock serials
    serials = []
    for batch in batches:
        for i in range(1, 3):  # 2 serials per batch for prototype
            serials.append({"serial": f"{batch['batch_id'].replace(' ', '')}-{str(i).zfill(6)}", "batch_id": batch['batch_id']})
    with open(os.path.join(mock_dir, 'serials.csv'), 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=serials[0].keys())
        writer.writeheader()  # Keep for inspection
        writer.writerows(serials)

    # Generate mock TMS data
    tms_data = []
    start_date = datetime(2025, 2, 1)
    for serial in serials:
        install_date = start_date + timedelta(days=random.randint(0, 28))
        usage_days = (datetime(2025, 9, 17) - install_date).days
        tms_data.append({
            "serial": serial["serial"],
            "track_id": f"Track {chr(88 + random.randint(0, 2))}",  # X, Y, Z
            "install_date": install_date.strftime("%Y-%m-%d"),
            "last_inspection_date": (install_date + timedelta(days=210)).strftime("%Y-%m-%d"),
            "fault": random.choice(["Wear", "Crack", "None"]),
            "usage_days": usage_days
        })
    with open(os.path.join(mock_dir, 'tms_data.csv'), 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=tms_data[0].keys())
        writer.writeheader()  # Keep for inspection
        writer.writerows(tms_data)

    # Import data into PostgreSQL (skip header row)
    def import_csv_without_header(cursor, filename, table, columns):
        with open(filename, 'r') as f:
            next(f)  # Skip the header row
            cursor.copy_from(f, table, sep=',', columns=columns)
        print(f"Imported data to {table}")

    import_csv_without_header(cursor, os.path.join(mock_dir, 'batches.csv'), 'batches', ('batch_id', 'vendor', 'supply_date', 'warranty_end', 'material_type', 'quantity'))
    import_csv_without_header(cursor, os.path.join(mock_dir, 'serials.csv'), 'serials', ('serial', 'batch_id'))
    import_csv_without_header(cursor, os.path.join(mock_dir, 'tms_data.csv'), 'tms_data', ('serial', 'track_id', 'install_date', 'last_inspection_date', 'fault', 'usage_days'))

    conn.commit()
    print("Mock data imported successfully!")

except psycopg2.Error as e:
    print(f"Error importing data: {e}")
except FileNotFoundError as e:
    print(f"CSV file not found: {e}")
finally:
    if conn:
        conn.close()
        print("Database connection closed.")