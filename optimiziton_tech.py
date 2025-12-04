# Comprehensive PySpark Optimization Guide
# 1. DataFrame API vs RDDs
# Always prefer DataFrames/Datasets over RDDs for better performance through Catalyst optimizer and Tungsten execution.

# python
# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName('Optimization') \
#     .config("spark.sql.adaptive.enabled", "true") \
#     .getOrCreate()

# # ❌ RDD approach (slow, no optimization)
# rdd = spark.sparkContext.parallelize([(1, 'A', 25), (2, 'B', 30), (3, 'C', 35)])
# rdd_mapped = rdd.map(lambda x: (x[0], x[1].lower(), x[2]))
# result = rdd_mapped.filter(lambda x: x[2] > 28)

# # ✅ DataFrame approach (optimized)
# df = spark.createDataFrame([(1, 'A', 25), (2, 'B', 30), (3, 'C', 35)], ['id', 'name', 'age'])
# df_optimized = df.filter(df.age > 28).withColumn("name_lower", lower(df.name))
# df_optimized.explain()  # View optimized execution plan
# 2. Caching and Persistence Strategies
# Cache strategically based on reuse patterns:

# python
# from pyspark.sql import SparkSession
# from pyspark.storagelevel import StorageLevel

# df = spark.read.parquet('large_dataset.parquet')

# # Cache with appropriate storage level
# df.cache()  # MEMORY_AND_DISK by default

# # Or specify storage level explicitly
# df.persist(StorageLevel.MEMORY_AND_DISK)  # For large datasets
# df.persist(StorageLevel.MEMORY_ONLY)      # For small datasets that fit in memory
# df.persist(StorageLevel.DISK_ONLY)        # When memory is constrained

# # Materialize the cache
# df.count()

# # Check if DataFrame is cached
# print(df.is_cached)

# # Unpersist when no longer needed
# df.unpersist()
# 3. Join Optimization Techniques
# Choose the right join strategy:

# python
# from pyspark.sql.functions import broadcast

# # Sample data
# large_df = spark.range(1000000).withColumn("category", (col("id") % 100).cast("integer"))
# small_df = spark.createDataFrame([(i, f"Category_{i}") for i in range(100)], ["cat_id", "cat_name"])

# # ✅ Broadcast Hash Join (for small tables < 10MB)
# optimized_join = large_df.join(broadcast(small_df), large_df.category == small_df.cat_id)

# # ✅ Sort Merge Join (for large tables)
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")  # Default 10MB
# auto_join = large_df.join(small_df, large_df.category == small_df.cat_id)

# # Check join execution plan
# optimized_join.explain()

# # For skewed data joins
# from pyspark.sql.functions import rand
# skewed_join = large_df.withColumn("salt", (rand() * 10).cast("int")) \
#                      .join(small_df.withColumn("salt", explode(array([lit(i) for i in range(10)]))),
#                            [large_df.category == small_df.cat_id, "salt"])
# 4. Partition Management
# Optimize data distribution:

# python
# # Read with partitioning
# df = spark.read.parquet("partitioned_data/")

# # Check current partitions
# print("Current partitions:", df.rdd.getNumPartitions())

# # ✅ Repartition by column (for better distribution)
# df_repartitioned = df.repartition(100, "partition_col")

# # ✅ Coalesce (reduce partitions without shuffle)
# df_coalesced = df.coalesce(10)

# # ✅ Repartition for writing
# df.write.option("maxRecordsPerFile", 100000) \
#        .mode("overwrite") \
#        .partitionBy("year", "month") \
#        .parquet("output/")

# # Adaptive Query Execution (AQE) - Spark 3.0+
# spark.conf.set("spark.sql.adaptive.enabled", "true")
# spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# spark.conf.set("spark.sql.adaptive.skew.enabled", "true")
# 5. Predicate Pushdown and Column Pruning
# Minimize data read:

# python
# # ✅ Efficient filtering and projection
# df_optimized = spark.read.parquet("large_table.parquet") \
#     .select("id", "name", "salary") \          # Column pruning
#     .filter("department = 'Engineering'") \    # Predicate pushdown
#     .filter("salary > 50000") \
#     .filter("start_date > '2020-01-01'")

# # Partition pruning
# df_partition_pruned = spark.read.parquet("partitioned_data/") \
#     .filter("year = 2023 AND month = 12")

# # Check what gets pushed down
# df_optimized.explain()
# 6. File Format Optimization
# Choose optimal file formats:

# python
# # Writing optimized files
# df.write \
#   .option("compression", "snappy") \    # or "gzip", "lzo"
#   .option("parquet.block.size", 134217728) \  # 128MB blocks
#   .mode("overwrite") \
#   .parquet("optimized_data/")

# # For better compression
# df.write \
#   .option("compression", "gzip") \
#   .mode("overwrite") \
#   .parquet("high_compression_data/")

# # Reading with optimizations
# df = spark.read \
#     .option("mergeSchema", "true") \
#     .parquet("data/*.parquet")
# 7. Built-in Functions vs UDFs
# Avoid UDFs when possible:

# python
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # ❌ Python UDF (slow - serialization overhead)
# @udf(StringType())
# def extract_domain_udf(email):
#     return email.split('@')[-1] if '@' in email else None

# df_slow = df.withColumn("domain", extract_domain_udf(col("email")))

# # ✅ Built-in functions (fast)
# df_fast = df.withColumn("domain", split(col("email"), "@").getItem(1))

# # ✅ SQL expressions
# df_faster = df.withColumn("domain", expr("split(email, '@')[1]"))

# # ✅ Pandas UDF (Vectorized - better performance)
# from pyspark.sql.functions import pandas_udf

# @pandas_udf('string')
# def extract_domain_pandas(s: pd.Series) -> pd.Series:
#     return s.str.split('@').str[-1]

# df_pandas = df.withColumn("domain", extract_domain_pandas(col("email")))
# 8. Spark Configuration Tuning
# Optimize Spark settings:

# python
# # Create optimized Spark session
# spark = SparkSession.builder \
#     .appName("OptimizedApp") \
#     .config("spark.sql.adaptive.enabled", "true") \
#     .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
#     .config("spark.sql.adaptive.skew.enabled", "true") \
#     .config("spark.sql.shuffle.partitions", "200") \          # Default 200
#     .config("spark.sql.autoBroadcastJoinThreshold", "50MB") \ # Default 10MB
#     .config("spark.executor.memory", "4g") \
#     .config("spark.driver.memory", "2g") \
#     .config("spark.executor.cores", "2") \
#     .config("spark.default.parallelism", "100") \
#     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
#     .getOrCreate()

# # Runtime configurations
# spark.conf.set("spark.sql.adaptive.enabled", "true")
# spark.conf.set("spark.sql.adaptive.skew.enabled", "true")
# 9. Aggregation Optimization
# Use efficient aggregations:

# python
# # ❌ Inefficient
# slow_agg = df.groupBy("category").agg(collect_list("value"))

# # ✅ Efficient aggregations
# fast_agg = df.groupBy("category").agg(
#     count("value").alias("count"),
#     sum("value").alias("total"),
#     avg("value").alias("average"),
#     max("value").alias("maximum")
# )

# # ✅ Approximate aggregations for large datasets
# from pyspark.sql.functions import approx_count_distinct
# approx_agg = df.agg(
#     approx_count_distinct("user_id", 0.05).alias("distinct_users")  # 5% error margin
# )
# 10. Data Skew Handling
# Address data skew issues:

# python
# from pyspark.sql.functions import *

# # Technique 1: Salting for skewed joins
# df_salted = df.withColumn("salt", (rand() * 10).cast("int"))

# # Technique 2: Broadcast small dimension tables
# small_dim = spark.read.parquet("small_dimension.parquet")
# result = large_fact.join(broadcast(small_dim), "key")

# # Technique 3: Separate skewed keys
# skewed_keys = df.filter("key in ('A', 'B', 'C')")  # Known skewed keys
# normal_keys = df.filter("key not in ('A', 'B', 'C')")

# # Technique 4: Increase shuffle partitions for skewed data
# spark.conf.set("spark.sql.adaptive.enabled", "true")
# spark.conf.set("spark.sql.adaptive.skew.enabled", "true")
# 11. Memory Management
# Optimize memory usage:

# python
# # Check DataFrame size
# df.cache()
# df.count()  # Materialize cache
# print("Storage level:", df.storageLevel)

# # Memory-efficient operations
# # Use columnar operations instead of row-based
# df.select("col1", "col2").filter("col1 > 100")  # Good
# # df.rdd.map(...)  # Avoid

# # Monitor memory usage through Spark UI
# # http://localhost:4040/storage/
# 12. Monitoring and Debugging
# Identify bottlenecks:

# python
# # Enable detailed logging
# spark.sparkContext.setLogLevel("INFO")

# # Check query execution plans
# df.explain()  # Physical plan
# df.explain(True)  # All plans (logical, optimized, physical)

# # Monitor through Spark UI
# # - Stages and tasks
# # - Storage tab for caching
# # - SQL tab for query analysis
# # - Environment tab for configurations

# # Use SparkListener for custom monitoring
# class MySparkListener(SparkListener):
#     def onTaskEnd(self, taskEnd):
#         print(f"Task {taskEnd.taskInfo.id} ended: {taskEnd.taskInfo.status}")

# spark.sparkContext.addSparkListener(MySparkListener())
# 13. Best Practices Summary
# python
# # ✅ DOs
# df_optimized = (spark.read.parquet("data.parquet")
#     .select("required_columns")           # Column pruning
#     .filter("conditions")                 # Predicate pushdown
#     .cache()                             # Cache if reused
#     .repartition("partition_col")        # Balanced partitions
#     .join(broadcast(small_df), "key")    # Broadcast small tables
# )

# # ✅ Use explain() to verify optimizations
# df_optimized.explain()

# # ✅ Write optimized output
# (df_optimized.write
#     .option("compression", "snappy")
#     .mode("overwrite")
#     .partitionBy("date_col")
#     .parquet("output/"))
# These optimization techniques can significantly improve PySpark performance by leveraging Catalyst optimizer, efficient memory usage, and proper data distribution strategies.

