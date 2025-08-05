with open('high_value_products.txt', 'w') as f:
    f.write("Product,Price\n")  # Header baru

    for item in data: #<-data ini ngerefer dari yg sbelumnya di latihan_2_1.py
        # show cuma yg diatas 100 aja
        if item['Price'] > 100.00:
            f.write(f"{item['Product']},{item['Price']:.2f}\n")