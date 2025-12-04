from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

df= spark.read.csv('C:/Users/Ashok/Desktop/files/Match_Data.csv', header=True ,inferSchema=True)
#df.show(5)

# df.createOrReplaceTempView('data')

# spark.sql("select player_of_match ,count(*) as total_count,rank() over(order by count(*) desc) as rnk from data group by player_of_match  order by total_count desc limit 5").show()

#sp=Window.orderBy(col("total_count").desc()).partitionBy('season')

#df.groupBy("player_of_match").agg(count(col('player_of_match')).alias('total_count')).orderBy(desc('total_count')).show(5)


#df.groupBy("player_of_match",'season').agg(count("player_of_match").alias("total_count")).\
# withColumn("rank", rank().over(sp)).orderBy(desc("total_count")).show(5)


# sp = Window.orderBy(desc("total_count"))

# df.groupBy("player_of_match") \
#   .agg(count("player_of_match").alias("total_count")) \
#   .withColumn("rank", rank().over(sp)) \
#   .orderBy(desc("total_count")) \
#   .show(5)


# sp = Window.partitionBy("season").orderBy(desc("total_count"))

# df.groupBy("player_of_match", "season").agg(count("player_of_match").alias("total_count")) \
#     .withColumn("rank", rank().over(sp)).where(col("rank") <= 2).orderBy(col("season"), col("rank")).show(5)

# sp = Window.partitionBy("season").orderBy(desc("total_count"))

# df.groupBy("player_of_match", "season").agg(count("player_of_match").alias("total_count")) \
#     .withColumn("d_rank", dense_rank().over(sp)).where(col("d_rank") <= 1).orderBy(col("season"),col("d_rank")).show()


df.groupBy("player_of_match").count().orderBy(desc('count')).show()
