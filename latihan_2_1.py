data = []

with open('data.txt', 'r') as f:
    header = f.readline().strip().split(',')  
    print(f"Header: {header}")
    
    for line in f:
        parts = line.strip().split(',')
        product = parts[0]
        quantity = int(parts[1])
        price = float(parts[2])
        
        print(f"Produk: {product}, Kuantitas: {quantity}")
        
        # Simpan ke dalam list of dict
        data.append({
            'Product': product,
            'Quantity': quantity,
            'Price': price
        })