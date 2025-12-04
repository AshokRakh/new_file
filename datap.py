
from pyspark.sql import SparkSession
import glob
import os

spark = SparkSession.builder.appName("FRS_PROJECT").config("spark.sql.streaming.schemaInference", "false").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


model_config =spark.read.csv(r'C:\\Users\\Ashok\\Desktop\\data_analysis\\data\\model_config.csv',header=True, inferSchema=True)

#model_config.show(5)

model_collateral = spark.read.csv(r"C:\\Users\\Ashok\\Desktop\\data_analysis\\data\\model_collateral.csv",header=True, inferSchema=True)

#model_collateral.show(5)


#df = spark.read.csv("C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/*.csv", header=True, inferSchema=True)



#df = spark.read.option("header", "true").csv("C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/*.csv")

path = "C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/"
files = glob.glob(os.path.join(path, "*.csv"))

#model_authorrep = spark.read.csv(all_files)
#model_authorrep = spark.read .option("header", "true").csv(files)
model_authorrep = spark.read.option("header", "true").option("inferSchema", "true").option("mode", "PERMISSIVE").csv(files)



#model_authorrep= spark.read.csv(files,header=True, inferSchema=True)

# print(model_authorrep.count())
# print(len(model_authorrep.columns))
# print(model_authorrep.columns)

# model_authorrep.show(10)
# print(f"Partitions: {model_authorrep.rdd.getNumPartitions()}
# model_authorrep.printSchema()

