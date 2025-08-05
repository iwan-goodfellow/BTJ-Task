import paramiko
import csv
import os

# --- KONFIG ---
HOSTNAME = '5.189.154.248'
PORT = 22
USERNAME = 'heri'
PASSWORD = 'Passwd093'
REMOTE_PATH = 'uploads/sales_data.csv'
LOCAL_FILE = 'sales_data.csv'
UPLOAD_PATH = 'uploads/ridwan_sales_data.csv' 

# --- KONEKSI & PROSES ---
try:
    print("🛜Menghubungkan ke server SFTP...")
    transport = paramiko.Transport((HOSTNAME, PORT))
    transport.connect(username=USERNAME, password=PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("✅ Koneksi berhasil.")

    # --- DOWNLOAD FILE sales_data ---
    print("⬇Mengunduh file dari server...")
    sftp.get(REMOTE_PATH, LOCAL_FILE)
    print(f"✅ File '{LOCAL_FILE}' berhasil diunduh.")

    # --- BACA DAN PROSES FILE ---
    print("\nMembaca isi file dan menghitung total...")
    data_with_total = []

    with open(LOCAL_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                product = row['ProductName']
                quantity = int(row['QuantitySold'])
                price = float(row['Price'])
                total = quantity * price
                print(f"Produk: {product}, Kuantitas: {quantity}, Total: {total:.2f}")
                row['total_amount'] = f"{total:.2f}"
                data_with_total.append(row)
            except (ValueError, KeyError) as e:
                print(f"⚠️  Baris tidak valid dilewati: {row} ➜ {e}")

    if not data_with_total:
        raise ValueError("❌ File kosong atau tidak ada baris data yang valid.")

    # --- TULIS FILE HASIL ---
    print("\nMenulis file hasil dengan total_amount...")
    fieldnames = list(data_with_total[0].keys())
    with open('processed_sales_data.csv', 'w', newline='') as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data_with_total)
    print("✅ File lokal 'processed_sales_data.csv' berhasil dibuat.")

    # --- UPLOAD FILE HASIL ridwan_sales_data ---
    print("⬆️ Mengunggah file hasil ke server...")
    sftp.put('processed_sales_data.csv', UPLOAD_PATH)
    print(f"✅ File berhasil diupload ke: {UPLOAD_PATH}")

except paramiko.ssh_exception.AuthenticationException:
    print("❌ Gagal otentikasi: username atau password salah.")
except paramiko.ssh_exception.SSHException as ssh_err:
    print(f"❌ SSH error: {ssh_err}")
except FileNotFoundError as fnf:
    print(f"❌ File tidak ditemukan: {fnf}")
except ConnectionError:
    print("❌ Tidak bisa terhubung ke server.")
except Exception as e:
    print(f"❌ Error tidak terduga: {e}")

finally:
    try:
        sftp.close()
        transport.close()
        print("🔒 Koneksi SFTP ditutup.")
    except:
        pass
