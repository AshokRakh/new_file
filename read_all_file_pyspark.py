from pyspark.sql import SparkSession
import glob
import os

spark = SparkSession.builder.appName("ReadAllCSV").getOrCreate()
#spark.sparkContext.setLogLevel("ERROR")  #----> ignore massage

path = "C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/"
files = glob.glob(os.path.join(path, "*.csv"))

#df = spark.read.csv(all_files)
df = spark.read .option("header", "true").csv(files)

print(df.count())
print(len(df.columns))
print(df.columns)

# df.show(10)

# print(f"Columns: {df.columns}")
# print(f"Partitions: {df.rdd.getNumPartitions()}")

# d
# print("\n📈 First 10 rows:")
# df.show(10)

# print("\n🏗️ Schema:")
# df.printSchema()
