from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import reduce
from pyspark.sql import DataFrame
import os


spark = SparkSession.builder.appName("FRS_Project").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  #----> show error massage



def validate_dataframe(df,
                        n_cols=None,
                          check_duplicates=False, 
                          check_nulls=False):
    
    if n_cols and len(df.columns) != n_cols:
        return False, f"Expected {n_cols} columns but got {len(df.columns)}"
    
    if check_duplicates and df.count() != df.dropDuplicates().count():
        return False, "Duplicates found"
    
    if check_nulls and df.filter(df.isNull()).count() > 0:
        return False, "Null values found"
    
    return True, "DataFrame passed validation"

model_config = spark.read.csv("C:/Users/Ashok/Desktop/data_analysis/data/model_config.csv",header=True, inferSchema=True)
#model_config.show()

model_collateral = spark.read.csv("C:/Users/Ashok/Desktop/data_analysis/data/model_collateral.csv",header=True, inferSchema=True)

#model_collateral.show()

model_authorrep = spark.read.csv("C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/*.csv", header=True, inferSchema=True)

# model_authorrep.show(5)

# print(model_authorrep.columns)

# model_authorrep.printSchema()

# print("row_count",model_authorrep.count() )  #----> check row_count

# print("col_count",len(model_authorrep.columns))   #-----> check col_count

# print("Ashok_Rakh")

is_valid, message = validate_dataframe(model_config, n_cols=4, check_duplicates=True)
print(is_valid, message)

is_valid, message = validate_dataframe(model_collateral, n_cols=78, check_duplicates=True)
print(is_valid, message)


path = "C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/"
ssv = [f for f in os.listdir(path) if f.endswith(".csv")]

dfs = []
for file in ssv:
    df = spark.read.csv(os.path.join(path, file), header=True, inferSchema=True)
    dfs.append(df)

for df in dfs:
    is_valid, message = validate_dataframe(df, n_cols=14, check_duplicates=True)
print(is_valid, message)

df = reduce(DataFrame.unionByName, dfs) # Concatenate all DataFrames

print("row_count",df.count() )  #----> check row_count

print("col_count",len(df.columns))   #-----> check col_count


#ECL report:-

# Stage 1 ECL

df = df.withColumn("stage1ecl", col("EAD") * col("PD12") * col("LGD"))

# # Stage 2 ECL

df = df.withColumn("stage2ecl", col("EAD") * col("PDLT") * col("LGD"))

# # Stage 3 ECL

df = df.withColumn("stage3ecl", col("EAD") * col("LGD"))

ecl_dataframe = df.select("EAD", "PD12", "LGD", "PDLT", "stage1ecl", "stage2ecl", "stage3ecl")

ecl_dataframe.show(5)

#ecl_dataframe.coalesce(1).write.option("header", "true").mode("overwrite").parquet("C:/Users/Ashok/Desktop/Project_File/ecl_dataframe_csv")
ecl_dataframe.coalesce(1).write.option("header", "true").mode("overwrite").csv("C:/Users/Ashok/Desktop/Project_File/ecl_dataframe_csv")

#.write.mode("overwrite").option("header", True).parquet("C:/Users/Ashok/Desktop/Project_File/ecl_dataframe1_csv")

#ead variation reports:-

# change_EAD = EAD - Previous EAD

df = df.withColumn("change_EAD", col("EAD") - col("Previous EAD"))

# percentage_change_EAD = ((EAD - Previous EAD) / Previous EAD) * 100

df = df.withColumn("percentage_change_EAD",((col("EAD") - col("Previous EAD")) / col("Previous EAD")) * 100)

EAD_DF = df.select("EAD", "Previous EAD", "change_EAD", "percentage_change_EAD")

EAD_DF.show(5)

EAD_DF.coalesce(1).write.option("header", "true").mode("overwrite").csv("C:/Users/Ashok/Desktop/Project_File/EAD_DF_csv")


#EAD_DF.coalesce(1).write.option("header", "true").mode("overwrite").parquet("C:/Users/Ashok/Desktop/Project_File/EAD_DF_csv")


#EAD_DF.write.mode("overwrite").option("header", True).parquet("C:/Users/Ashok/Desktop/Project_File/EAD_DF1_csv")


#LGD variation reports:-

# change_LGD = LGD - Previous LGD
df = df.withColumn("change_LGD", col("LGD") - col("Previous LGD"))

# percentage_change_LGD = ((LGD - Previous LGD) / Previous LGD) * 100
df = df.withColumn("percentage_change_LGD",((col("LGD") - col("Previous LGD")) / col("Previous LGD")) * 100)

LGD_DF = df.select("LGD", "Previous LGD", "change_LGD", "percentage_change_LGD")

LGD_DF.show(5)

LGD_DF.coalesce(1).write.option("header", "true").mode("overwrite").csv("C:/Users/Ashok/Desktop/Project_File/LGD_DF_csv")
# LGD_DF.coalesce(1).write.option("header", "true").mode("overwrite").parquet("C:/Users/Ashok/Desktop/Project_File/LGD_DF_csv")

# LGD_DF.write.mode("overwrite").option("header", True).parquet("C:/Users/Ashok/Desktop/Project_File/LGD_DF1_csv")



print("All project run successfully")
















