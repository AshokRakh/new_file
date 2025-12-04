from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

df= spark.read.csv('C:/Users/Ashok/Desktop/files/Match_Data.csv', header=True ,inferSchema=True)
#df.show(5)

#df.select('season','date','city','team1').show(5)

# print('total_rows',df.count())

# print(df.columns)

# print(len(df.columns))

a = df.withColumn('new_Ashok',col('win_by_runs') + col('win_by_wickets'))
# a.select('city','season','new_Ashok').show()

# a.agg(sum('new_Ashok')).show()

# a.agg(min('new_Ashok')).show()

# a.agg(max('new_Ashok')).show()

