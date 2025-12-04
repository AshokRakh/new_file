from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import os
from functools import reduce
from pyspark.sql import DataFrame

spark = SparkSession.builder.appName("FRS_PROJECT").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  #----> ignore error massage

model_config =spark.read.csv(r'C:\\Users\\Ashok\\Desktop\\data_analysis\\data\\model_config.csv',header=True, inferSchema=True)

#model_config.show()

model_collateral = spark.read.csv(r"C:\\Users\\Ashok\\Desktop\\data_analysis\\data\\model_collateral.csv",header=True, inferSchema=True)

#model_collateral.show(5)


model_authorrep = spark.read.csv(r"C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/*.csv", header=True, inferSchema=True)

#model_authorrep.show(5)

#print(model_authorrep.columns)

# print(model_authorrep.rdd.getNumPartitions())

# model_authorrep.printSchema()

print("row_count",model_authorrep.count() )  #----> check row_count

print("col_count",len(model_authorrep.columns))   #-----> check col_count


#print(model_authorrep.select([count(when(col(c).isNull(), c)).alias(c) for c in model_authorrep.columns]))  #----> Quick null check for all columns

null_counts = model_authorrep.select([count(when(col(c).isNull(), c)).alias(c)for c in model_authorrep.columns])  # ----->Null values per column
null_counts.show()

total_rows = model_authorrep.count()
distinct_rows = model_authorrep.distinct().count()
print("distinct_rows",distinct_rows)
print("Total duplicate rows:",total_rows - distinct_rows)    #----> check duplicate_rows


path = "C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/"
ssv = [f for f in os.listdir(path) if f.endswith(".csv")]

dfs = []
for file in ssv:
    df = spark.read.csv(os.path.join(path, file), header=True, inferSchema=True)
    dfs.append(df)

df = reduce(DataFrame.unionByName, dfs) # Concatenate all DataFrames

#ECL report:-

# Stage 1 ECL
df = df.withColumn("stage1ecl", col("EAD") * col("PD12") * col("LGD"))

# Stage 2 ECL
df = df.withColumn("stage2ecl", col("EAD") * col("PDLT") * col("LGD"))

# Stage 3 ECL
df = df.withColumn("stage3ecl", col("EAD") * col("LGD"))

ecl_dataframe = df.select("EAD", "PD12", "LGD", "PDLT", "stage1ecl", "stage2ecl", "stage3ecl")

ecl_dataframe.show(5)

ecl_dataframe.coalesce(1).write.option("header", "true").mode("overwrite").csv("C:/Users/Ashok/Desktop/Project_File/ecl_dataframe_csv")

ecl_dataframe.write.mode("overwrite").option("header", True).csv("C:/Users/Ashok/Desktop/Project_File/ecl_dataframe1_csv")

#ead variation reports:-

# change_EAD = EAD - Previous EAD
df = df.withColumn("change_EAD", col("EAD") - col("Previous EAD"))

# percentage_change_EAD = ((EAD - Previous EAD) / Previous EAD) * 100
df = df.withColumn("percentage_change_EAD",((col("EAD") - col("Previous EAD")) / col("Previous EAD")) * 100)

EAD_DF = df.select("EAD", "Previous EAD", "change_EAD", "percentage_change_EAD")

# EAD_DF.show(5)

EAD_DF.coalesce(1).write.option("header", "true").mode("overwrite").csv("C:/Users/Ashok/Desktop/Project_File/EAD_DF_csv")

EAD_DF.write.mode("overwrite").option("header", True).csv("C:/Users/Ashok/Desktop/Project_File/EAD_DF1_csv")


#LGD variation reports:-

# change_LGD = LGD - Previous LGD
df = df.withColumn("change_LGD", col("LGD") - col("Previous LGD"))

# percentage_change_LGD = ((LGD - Previous LGD) / Previous LGD) * 100
df = df.withColumn("percentage_change_LGD", 
                     ((col("LGD") - col("Previous LGD")) / col("Previous LGD")) * 100)

LGD_DF = df.select("LGD", "Previous LGD", "change_LGD", "percentage_change_LGD")

LGD_DF.show(5)

LGD_DF.coalesce(1).write.option("header", "true").mode("overwrite").csv("C:/Users/Ashok/Desktop/Project_File/LGD_DF_csv")

LGD_DF.write.mode("overwrite").option("header", True).csv("C:/Users/Ashok/Desktop/Project_File/LGD_DF1_csv")














