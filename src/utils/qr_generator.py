import os
import qrcode
import psycopg2
from config.db_config import DB_CONFIG

def generate_qr_for_serial(serial, output_dir="data/qr_codes"):
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, f"{serial}.png")
    if not os.path.exists(filepath):  # Avoid overwriting existing QRs
        data = f"Serial: {serial}"
        qr = qrcode.QRCode(version=1, box_size=10, border=4)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill="black", back_color="white")
        img.save(filepath)
        print(f"QR generated for {serial}")
    else:
        print(f"QR for {serial} already exists, skipping")

def generate_qrs(new_serials=None, output_dir="data/qr_codes"):
    if new_serials:
        serials = new_serials
    else:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT serial FROM serials;")
        serials = [row[0] for row in cursor.fetchall()]
        conn.close()
    for serial in serials:
        generate_qr_for_serial(serial, output_dir)

if __name__ == "__main__":
    generate_qrs()