
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()

path=r"C:\\Users\\Ashok\\Desktop\\pyspark_file\\weather.parquet"

df = spark.read.parquet(path)

df.show()

# print("Total rows:", df.count())
# df.show(10, truncate=False)
# df.printSchema()



df.orderBy(df.Sunshine.desc()).show(5)


column_names = df.columns
print(column_names)
