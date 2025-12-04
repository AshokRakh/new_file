
# ***********************SPARK SQL**********************


# API -application peripheral interface (inbuilt functions)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySparkExample").getOrCreate()


#df = spark.read.csv(r"C:\\Users\\Ashok\\Desktop\\files\\Match_Data.csv", header=True, inferSchema=True)


# df = spark.read.parquet("path/to/file.parquet")


# df = spark.read.json("path/to/file.json")

# df.show(5)         # Show top 5 rows
# df.printSchema()   # Print schema
# df.columns         # List columns
# df.count()         # Row count


# df.select("id", "season").show()
# df.filter(df.id > 100).show()
# df.where(df["city"] == "Pune").show()


# ****************Use sql*********************

df = spark.read.csv(r"C:\\Users\\Ashok\\Desktop\\files\\Match_Data.csv", header=True, inferSchema=True)

#df.show()  #---to check data 

df.createOrReplaceTempView("match")

result=spark.sql("select * from match where city ='Pune'")
#result.show()

spark.sql("select player_of_match,count(player_of_match)as tcount from match group by player_of_match order by tcount desc limit 5")




spark.sql("""with new as (select
          season,
          winner, 
          count(winner)as total_winner,
          RANK() OVER(partition by season order by count(winner) desc) as rnc 
          from match
          group by season,winner)
         select * from new where rnc <=3""").show()




spark.sql("""SELECT 
          season,
          winner, 
          count(winner)as t_winner,
          RANK()OVER(partition by season order by count(winner) desc) as rnc 
          from match
          group by season,winner""")



spark.sql("""with ashok as (select
          season,player_of_match,
          count(player_of_match)as total_a,
          dense_rank() over(partition by season order by count(player_of_match) desc)as rnc
          from match 
          group by season,player_of_match)
          select * from ashok where rnc == 1 """).show()



# df1(id,name,address)
# df2(id,accountno,salary)

# df1.createOrReplaceTempView(emp1)
# df2.createOrReplaceTempView(emp2)

# result2=spark.sql("select a.name,a.address,b.accountno,b.salary from emp1 a join emp2 on a.id=b.id")

# result2.show()     








# *********************Next Session Discussion**********************



# from pyspark.sql.functions import col, lit, when

# df = df.withColumn("new_col", df.col1 * 2)
# df = df.withColumn("flag", when(df.col2 == "India", 1).otherwise(0))
# df = df.withColumnRenamed("old_col", "new_name")


# from pyspark.sql.functions import sum, avg, count

# df.groupBy("col2").agg(
#     sum("col1").alias("total"),
#     avg("col1").alias("average"),
#     count("*").alias("count")
# ).show()



# df.orderBy("col1", ascending=False).show()



# df1.join(df2, on="id", how="inner").show()
# df1.join(df2, df1.id == df2.emp_id, "left").show()


# from pyspark.sql.window import Window
# from pyspark.sql.functions import row_number

# windowSpec = Window.partitionBy("col2").orderBy("col1")
# df.withColumn("row_num", row_number().over(windowSpec)).show()

# df.write.csv("path/to/output.csv", header=True)
# df.write.parquet("path/to/output.parquet")
# df.write.mode("overwrite").json("path/to/output.json")








# rdd = spark.sparkContext.textFile("path/to/file.txt")
# df = rdd.map(lambda x: x.split(",")).toDF(["col1", "col2"])


