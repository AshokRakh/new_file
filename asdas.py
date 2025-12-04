from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, when

# Spark session
spark = SparkSession.builder.appName("DataValidation_FillNA").getOrCreate()

# CSV Read
df = spark.read.csv(r'C:\Users\Ashok\Desktop\data_analysis\data\model_config.csv', 
                    header=True, inferSchema=True)

# ------------------------
# 1) Initial Data Check
# ------------------------
print("=== Top 5 Rows ===")
df.show(5)

print("=== Schema ===")
df.printSchema()

# Total rows
print(f"Total Rows: {df.count()}")

# Null/NaN values per column
print("=== Null/NaN Values ===")
df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).show()

# ------------------------
# 2) Fill Missing Values
# ------------------------
# Suppose 'marks' numeric, 'city' string, 'name' string
df = df.fillna({
    'marks': 0,       # numeric columns
    'score': 0,       # another numeric column
    'city': 'Unknown', 
    'name': 'NoName'
})

# Verify nulls are filled
print("=== Nulls After fillna ===")
df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).show()

# ------------------------
# 3) Duplicate Rows Check
# ------------------------
duplicate_count = df.count() - df.dropDuplicates().count()
print(f"Duplicate Rows: {duplicate_count}")

# ------------------------
# 4) Summary Statistics
# ------------------------
df.describe().show()

# ------------------------
# 5) Optional: Unique key validation (if 'id' column exists)
# ------------------------
if 'id' in df.columns:
    unique_count = df.select('id').distinct().count()
    print(f"Unique IDs: {unique_count}")
