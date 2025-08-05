import paramiko
import csv

class SFTProcessor:
    def __init__(self,name):
        self.name = name
        self.hostname = '5.189.154.248'
        self.port = 22
        self.username = 'heri'
        self.password = 'Passwd093'
        self.remote_path = 'uploads/sales_data.csv'
        self.local_file = 'sales_data.csv'
        self.output_file = 'processed_sales_data.csv'
        self.upload_path = 'uploads/refactor_ridwan_sales_data.csv'
        self.transport = None
        self.sftp = None
        print('Koneksi ke FTTP...')

        try:
            self.transport = paramiko.Transport((self.hostname, self.port))
            self.transport.connect(username=self.username, password=self.password)
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            print("Koneksi SFTP berhasil.")
        except Exception as e:
            print(f"Gagal koneksi SFTP: {e}")

    def download_file_dari_sftp(self):
        try:
            print("Mengunduh file dari server...")
            self.sftp.get(self.remote_path, self.local_file)
            print(f"File '{self.local_file}' berhasil diunduh.")
        except Exception as e:
            print(f"Gagal mengunduh file: {e}")
    
    def transform_file(self):
        print("\nTransform data...")
        self.processed_data = []

        try:
            with open(self.local_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        product = row['ProductName']
                        quantity = int(row['QuantitySold'])
                        price = float(row['Price'])
                        total = quantity * price
                        print(f"Produk: {product}, Kuantitas: {quantity}, Total: {total:.2f}")
                        row['total_amount'] = f"{total:.2f}"
                        self.processed_data.append(row)
                    except (ValueError, KeyError) as e:
                        print(f"Baris dilewati: {row} ➜ {e}")
            
            if not self.processed_data:
                raise ValueError("File kosong atau semua baris invalid.")

            # Tulis ke file hasil
            fieldnames = list(self.processed_data[0].keys())
            with open(self.output_file, 'w', newline='') as f_out:
                writer = csv.DictWriter(f_out, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.processed_data)
            print(f"File hasil '{self.output_file}' berhasil dibuat.")

        except Exception as e:
            print(f"Gagal transformasi file: {e}")
        
    def load_to_sftp(self):
        try:
            print("Upload file ke server...")
            self.sftp.put(self.output_file, self.upload_path)
            print(f"File berhasil diupload ke: {self.upload_path}")
        except Exception as e:
            print(f"Gagal upload file: {e}")

    def close(self):
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()
        print("Koneksi SFTP ditutup.")


# buat objek processor
processor = SFTProcessor("Ridwan")
# show sesuain urutan
processor.download_file_dari_sftp()
processor.transform_file()
processor.load_to_sftp()
processor.close()