from pyspark.sql import SparkSession
import random
import datetime

spark = SparkSession.builder \
    .appName("EcommerceOrders") \
    .getOrCreate()

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
    
    data.append((order_id, customer_id, product, category, quantity, price, order_date, country))

columns = ["OrderID", "CustomerID", "Product", "Category", "Quantity", "Price", "OrderDate", "Country"]

df = spark.createDataFrame(data, columns)


df.write.mode("overwrite").parquet("C:/Users/Ashok/Desktop/data/ecommerce_orders.parquet")

print("✅ E-commerce Orders dataset parquet मध्ये save झाला!")
