from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, max, min

spark = SparkSession.builder .appName("PySparkT").getOrCreate()


dict_RDD =[("omkar",25,"302 satara"),("ashok",26,"Beed")]
columns=["name","age","address"]
expo=spark.createDataFrame(dict_RDD,columns)
print(expo.collect())
expo.show()


# data = [(1,"Ashok", 25), (2,"Vaishnavi",23),(3,"Omkar", 30), (4,"Kamlja", 35),(5,"Chaitrali",28),(6,"Komal",26)]
# columns = ["Id","Name", "Age"]
# df = spark.createDataFrame(data, columns)

# df.show()

# df.select("Name").show()

# df.filter(df.Age > 28).show()

# df.withColumn("Year", 2025 - df.Age).show()

# # विविध एग्रीगेशन फंक्शन्स
# agg_results = df.groupBy("Age").agg(
#     count("*").alias("Total_Count"),
#     sum("Age").alias("Total_sum"),
#     avg("Age").alias("Avg_a"),
#     max("Age").alias("Max_A"),
#     min("Age").alias("Min_A")
# )
# agg_results.show()

# sc = spark.sparkContext

# data = [1, 2, 3, 4, 5]
# rdd = sc.parallelize(data)

# rdd_map = rdd.map(lambda x: x * 2)         
# rdd_filter = rdd.filter(lambda x: x > 2)  

# # print(rdd.collect())        
# # print(rdd.count())          
# # print(rdd.first())