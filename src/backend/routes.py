from flask import jsonify, request
from . import app  # Import the app instance from __init__.py (or app.py)
import psycopg2
import random
from datetime import datetime, timedelta
from src.utils.qr_generator import generate_qrs

def get_db_connection():
    from config.db_config import DB_CONFIG
    return psycopg2.connect(**DB_CONFIG)

# Endpoint to get all batches
@app.route('/batches')
def get_batches():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT batch_id, vendor, supply_date, warranty_end, material_type, quantity FROM batches;")
    batches = [{"batch_id": row[0], "vendor": row[1], "supply_date": row[2], "warranty_end": row[3], "material_type": row[4], "quantity": row[5]} for row in cursor.fetchall()]
    conn.close()
    return jsonify({"batches": batches})

# Endpoint to get all serials
@app.route('/serials')
def get_serials():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT serial, batch_id FROM serials;")
    serials = [{"serial": row[0], "batch_id": row[1]} for row in cursor.fetchall()]
    conn.close()
    return jsonify({"serials": serials})

# Endpoint to get all tms_data
@app.route('/tms_data')
def get_tms_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT serial, track_id, install_date, last_inspection_date, fault, usage_days FROM tms_data;")
    tms_data = [{"serial": row[0], "track_id": row[1], "install_date": row[2], "last_inspection_date": row[3], "fault": row[4], "usage_days": row[5]} for row in cursor.fetchall()]
    conn.close()
    return jsonify({"tms_data": tms_data})

# Endpoint to sync UDM/TMS data with quantity-based serials
@app.route('/sync_udm_tms_data', methods=['POST'])
def sync_udm_tms_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data provided"}), 400
    batch_id = data.get("batch_id")
    quantity = data.get("quantity")
    vendor = data.get("vendor")
    supply_date = data.get("supply_date")

    if not all([batch_id, quantity, vendor, supply_date]):
        return jsonify({"error": "Missing required fields (batch_id, quantity, vendor, supply_date)"}), 400

    try:
        quantity = int(quantity)
        if quantity <= 0:
            return jsonify({"error": "Quantity must be positive"}), 400
    except ValueError:
        return jsonify({"error": "Quantity must be an integer"}), 400

    new_udm_data = [
        (batch_id, vendor, supply_date,
         (datetime.strptime(supply_date, "%Y-%m-%d") + timedelta(days=random.randint(365, 730))).strftime("%Y-%m-%d"),
         random.choice(["Steel Clip", "Rubber Pad", "Composite"]), quantity)
    ]
    cursor.executemany("""
        INSERT INTO batches (batch_id, vendor, supply_date, warranty_end, material_type, quantity)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (batch_id) DO UPDATE SET quantity = EXCLUDED.quantity, vendor = EXCLUDED.vendor,
        supply_date = EXCLUDED.supply_date, warranty_end = EXCLUDED.warranty_end,
        material_type = EXCLUDED.material_type;
    """, new_udm_data)

    new_serials = []
    for i in range(quantity):
        serial = f"{batch_id.replace(' ', '')}-{str(i+1).zfill(6)}"
        new_serials.append((serial, batch_id))
    cursor.executemany("INSERT INTO serials (serial, batch_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;", new_serials)

    new_tms_data = []
    for serial in new_serials:
        if serial[0]:
            install_date = (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 180))).strftime("%Y-%m-%d")
            last_inspection = (datetime.strptime(install_date, "%Y-%m-%d") + timedelta(days=random.randint(90, 270))).strftime("%Y-%m-%d")
            fault = random.choices(["Wear", "Crack", "None"], weights=[15, 15, 70])[0]
            usage_days = (datetime(2025, 9, 19) - datetime.strptime(install_date, "%Y-%m-%d")).days
            track_id = f"Track {chr(88 + random.randint(0, 2))}"
            new_tms_data.append((serial[0], track_id, install_date, last_inspection, fault, usage_days))
    cursor.executemany("""
        INSERT INTO tms_data (serial, track_id, install_date, last_inspection_date, fault, usage_days)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (serial) DO UPDATE SET track_id = EXCLUDED.track_id, install_date = EXCLUDED.install_date,
        last_inspection_date = EXCLUDED.last_inspection_date, fault = EXCLUDED.fault, usage_days = EXCLUDED.usage_days;
    """, new_tms_data)

    conn.commit()
    generate_qrs(new_serials=[s[0] for s in new_serials])
    conn.close()

    return jsonify({"message": f"Synced 1 new UDM batch with {quantity} serials and {len(new_tms_data)} TMS entries! QRs generated for new batch."})