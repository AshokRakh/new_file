from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()

path=r"C:\\Users\\Ashok\\Desktop\\pyspark_file\\flights-1m.parquet"

df = spark.read.parquet(path)
df.show()

print("Total rows:", df.count())
df.show(10, truncate=False)
df.printSchema()
