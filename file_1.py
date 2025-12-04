from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()

path=r"C:\\Users\\Ashok\\Desktop\\Data_E\\pyspark_file\\house-price.parquet"

df = spark.read.parquet(path)

# print("Total rows:", df.count())
# df.show(10, truncate=False)
# df.printSchema()

df.repartition(4, "bedrooms").write.mode("overwrite").csv("C:/Users/Ashok/Desktop/Project_File/bedrooms")
