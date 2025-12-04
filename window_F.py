from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, dense_rank, ntile, lag, lead, sum, avg, min, max
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

# Sample data
data = [
    ("A", "2023-01-01", 100),
    ("A", "2023-01-02", 200),
    ("A", "2023-01-03", 300),
    ("B", "2023-01-01", 400),
    ("B", "2023-01-02", 500),
    ("B", "2023-01-03", 600),
    ("C", "2023-01-01", 700),
    ("C", "2023-01-02", 800),
    ("C", "2023-01-03", 900),
]

df = spark.createDataFrame(data, ["Category", "Date", "Sales"])
#df.show()

w = Window.partitionBy("Category").orderBy("Date")

df.withColumn("row_number", row_number().over(w)).show()

df.withColumn("rank", rank().over(w)).show()

df.withColumn("dense_rank", dense_rank().over(w)).show()

df.withColumn("ntile_2", ntile(2).over(w)).show()

df.withColumn("prev_sales", lag("Sales", 1).over(w)).show()

df.withColumn("next_sales", lead("Sales", 1).over(w)).show()

df.withColumn("running_sum", sum("Sales").over(w)).show()

win_spec = w.rowsBetween(-1, 1)  # previous, current, next
df.withColumn("moving_avg", avg("Sales").over(win_spec)).show()

df.withColumn("min_sales", min("Sales").over(w)) .withColumn("max_sales", max("Sales").over(w)).show()

df.withColumn("cum_avg", avg("Sales").over(w)).show()





# from pyspark.sql import SparkSession

# from pyspark.sql.functions import col, row_number, rank, dense_rank, ntile, \
#     lag, lead, sum, avg, min, max
# from pyspark.sql.window import Window

# # Spark Session
# spark = SparkSession.builder.appName("AllWindowFunctions").getOrCreate()

# # Sample Data
# data = [
#     ("A", "2023-01-01", 100),
#     ("A", "2023-01-02", 200),
#     ("A", "2023-01-03", 300),
#     ("B", "2023-01-01", 400),
#     ("B", "2023-01-02", 500),
#     ("B", "2023-01-03", 600),
#     ("C", "2023-01-01", 700),
#     ("C", "2023-01-02", 800),
#     ("C", "2023-01-03", 900),
# ]

# df = spark.createDataFrame(data, ["Category", "Date", "Sales"])
# df.show()

# # Window Spec
# w = Window.partitionBy("Category").orderBy("Date")
# w2 = w.rowsBetween(-1, 1)   # Moving average साठी



# # Apply All Window Functions
# result = df \
#     .withColumn("row_number", row_number().over(w)) \
#     .withColumn("rank", rank().over(w)) \
#     .withColumn("dense_rank", dense_rank().over(w)) \
#     .withColumn("ntile_2", ntile(2).over(w)) \
#     .withColumn("lag_sales", lag("Sales", 1).over(w)) \
#     .withColumn("lead_sales", lead("Sales", 1).over(w)) \
#     .withColumn("running_sum", sum("Sales").over(w)) \
#     .withColumn("cum_avg", avg("Sales").over(w)) \
#     .withColumn("moving_avg", avg("Sales").over(w2)) \
#     .withColumn("min_sales", min("Sales").over(w)) \
#     .withColumn("max_sales", max("Sales").over(w))

# #result.show(truncate=False)



