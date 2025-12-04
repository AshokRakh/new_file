from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import reduce
from pyspark.sql import DataFrame
import os

# 1️⃣ Create SparkSession with Hive support
spark = SparkSession.builder \
    .appName("FRS Project") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # Reduce console logs

# 2️⃣ DataFrame Validation Function
def validate_dataframe(df, n_cols=None, check_duplicates=False, check_nulls=False):
    if n_cols and len(df.columns) != n_cols:
        return False, f"Expected {n_cols} columns but got {len(df.columns)}"
    if check_duplicates and df.count() != df.dropDuplicates().count():
        return False, "Duplicates found"
    if check_nulls and df.filter(df.isNull()).count() > 0:
        return False, "Null values found"
    return True, "DataFrame passed validation"

# 3️⃣ Read CSV Files
model_config = spark.read.csv("C:/Users/Ashok/Desktop/data_analysis/data/model_config.csv", header=True, inferSchema=True)
model_collateral = spark.read.csv("C:/Users/Ashok/Desktop/data_analysis/data/model_collateral.csv", header=True, inferSchema=True)
model_authorrep = spark.read.csv("C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/*.csv", header=True, inferSchema=True)

# 4️⃣ Validation Checks
is_valid, message = validate_dataframe(model_config, n_cols=4, check_duplicates=True)
print("model_config:", is_valid, message)

is_valid, message = validate_dataframe(model_collateral, n_cols=78, check_duplicates=True)
print("model_collateral:", is_valid, message)

# 5️⃣ Merge multiple CSV files for author report
path = "C:/Users/Ashok/Desktop/data_analysis/data/model_auth_Rep/"
ssv = [f for f in os.listdir(path) if f.endswith(".csv")]
dfs = [spark.read.csv(os.path.join(path, f), header=True, inferSchema=True) for f in ssv]

for df_temp in dfs:
    is_valid, message = validate_dataframe(df_temp, n_cols=14, check_duplicates=True)
    print("AuthorRep file:", is_valid, message)

# Merge all author report CSVs
df = reduce(DataFrame.unionByName, dfs)

print("row_count:", df.count())
print("col_count:", len(df.columns))

# 6️⃣ ECL Calculations
df = df.withColumn("stage1ecl", col("EAD") * col("PD12") * col("LGD"))
df = df.withColumn("stage2ecl", col("EAD") * col("PDLT") * col("LGD"))
df = df.withColumn("stage3ecl", col("EAD") * col("LGD"))

ecl_dataframe = df.select("EAD", "PD12", "LGD", "PDLT", "stage1ecl", "stage2ecl", "stage3ecl")
ecl_dataframe.show(5)

# 7️⃣ EAD Variation
df = df.withColumn("change_EAD", col("EAD") - col("Previous EAD"))
df = df.withColumn("percentage_change_EAD", ((col("EAD") - col("Previous EAD")) / col("Previous EAD")) * 100)

EAD_DF = df.select("EAD", "Previous EAD", "change_EAD", "percentage_change_EAD")
EAD_DF.show(5)

# 8️⃣ LGD Variation
df = df.withColumn("change_LGD", col("LGD") - col("Previous LGD"))
df = df.withColumn("percentage_change_LGD", ((col("LGD") - col("Previous LGD")) / col("Previous LGD")) * 100)

LGD_DF = df.select("LGD", "Previous LGD", "change_LGD", "percentage_change_LGD")
LGD_DF.show(5)

# 9️⃣ Save reports as CSV locally
# output_path = "C:/Users/Ashok/Desktop/Project_File/"

# ecl_dataframe.coalesce(1).write.option("header", True).mode("overwrite").csv(os.path.join(output_path, "ecl_dataframe_csv"))
# EAD_DF.coalesce(1).write.option("header", True).mode("overwrite").csv(os.path.join(output_path, "EAD_DF_csv"))
# LGD_DF.coalesce(1).write.option("header", True).mode("overwrite").csv(os.path.join(output_path, "LGD_DF_csv"))

# 🔟 Save reports to Hive Internal Tables
spark.sql("CREATE DATABASE IF NOT EXISTS frs_db")
spark.sql("USE frs_db")

ecl_dataframe.write.mode("overwrite").saveAsTable("frs_db.ecl_report")
EAD_DF.write.mode("overwrite").saveAsTable("frs_db.ead_variation")
LGD_DF.write.mode("overwrite").saveAsTable("frs_db.lgd_variation")

# ✅ Completion Message
print("All project run successfully ✅")
