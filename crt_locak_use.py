# trial.py - Pandas वापरून Parquet तयार करणे
import pandas as pd
import random
import datetime

def random_date(start, end):
    return start + datetime.timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

rows = 500
start_date = datetime.datetime(2022, 1, 1)
end_date = datetime.datetime(2024, 12, 31)

data = []
for i in range(rows):
    order_id = i + 1
    customer_id = random.randint(1000, 9999)
    product = random.choice(["Laptop", "Mobile", "Shoes", "Book", "Headphones", "Watch"])
    category = random.choice(["Electronics", "Fashion", "Books", "Accessories"])
    quantity = random.randint(1, 5)
    price = round(random.uniform(10, 2000), 2)
    order_date = random_date(start_date, end_date)
    country = random.choice(["India", "USA", "UK", "Canada", "Germany"])
    
    data.append({
        "OrderID": order_id,
        "CustomerID": customer_id, 
        "Product": product,
        "Category": category,
        "Quantity": quantity,
        "Price": price,
        "OrderDate": order_date,
        "Country": country
    })

# Pandas DataFrame तयार करा
df = pd.DataFrame(data)

# Parquet मध्ये save करा
output_path = "C:/Users/Ashok/Desktop/data/ecommerce_orders.parquet"
df.to_parquet(output_path, engine='pyarrow')

print("✅ Pandas वापरून Parquet फाईल तयार झाली!")
print(f"फाईल location: {output_path}")
print(f"डेटा shape: {df.shape}")
print("\nपहिल्या 5 rows:")
print(df.head())
