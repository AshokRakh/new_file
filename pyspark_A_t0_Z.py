from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("pyspark_A_to_z").getOrCreate()

# data=[(1,'ashok','pune',20000),(2,'omkar','satara',25000),(3,'kamalja','pune',23000),(4,'chaitrali','nagar',30000),(5,'Ankita','satara',21000),(6,'vaishnavi','pune',40000),(7,'ruturaj','satara',25000),(8,'sujay','pune',23000),(9,'ankush','nagar',30000)]
# column=['id','name','city','salary']

# df=spark.createDataFrame(data,column)
# df.show()

# a=df.agg(min('salary')).show()

# a1=df.agg(max('salary')).show()

# a2=df.agg(avg('salary')).show()

# print('all agg function')

# a3=df.agg(
#     (min('salary').alias('min_salary')),
#     (max('salary').alias('max_salary')),
#     (avg('salary').alias('avg_salary'))
# ).show()

# a4=df.orderBy(desc('salary')).show()

# df.printSchema()

# dff = df.withColumn("id", df.id.cast("int"))
# dff1 = dff.withColumn("salary",col('salary').cast("double"))

# dff1.printSchema()
# d=df.select('id').show()

# d1=df.select('id','name').show()

# df1=df.groupBy('city').agg(sum('salary'))

# df1.show()

# df2=df.filter('salary < 25000').show()

# df3=df.where("city== 'pune'").show()

# df4=df.withColumnRenamed('city','new_city').show()  ##-----> rename column

# df4=df.withColumnsRenamed({'city':'new_city','name':'fname'}).show()  ##----> rename multiple column

# df5=df.withColumn('update_salary',col('salary')+ 10000)

#df5.show()

#df5.withColumn('add_salary',df5.salary + df5.update_salary).show()
# df5.withColumn('in_salary',df5.salary * 0.5).show()

#print('add two column 1 time ')
#WF=df5.withColumns({'new1':col('salary')*0.10 ,'new2':col('salary')*0.20})

# WF.show()

# WFF=Window.orderBy(desc('salary'))

# w1=WF.withColumn('rank',rank().over(WFF))
# w1.show()

# w2=w1.withColumn('rank1',rank().over(WFF)).where('rank==2')
# w2.show()

# w3=w1.withColumn('dense_rank',dense_rank().over(WFF))
# w3.show()

# w4=w3.withColumn('dense_rank1',dense_rank().over(WFF)).where('dense_rank==2')
# w4.show()

# w5=w3.withColumn('row_number',row_number().over(WFF))
# w5.show()


# w6=w5.withColumn('row_number1',row_number().over(WFF)).where('row_number==2')
# w6.show()



# WFF=Window.partitionBy('city').orderBy(desc('salary'))

# w1=WF.withColumn('rank',rank().over(WFF))
# w1.show()

# w2=w1.withColumn('rank1',rank().over(WFF)).where('rank==2')
# w2.show()

# w3=w1.withColumn('dense_rank',dense_rank().over(WFF))
# w3.show()

# w4=w3.withColumn('dense_rank1',dense_rank().over(WFF)).where('dense_rank==2')
# w4.show()

# w5=w3.withColumn('row_number',row_number().over(WFF))
# w5.show()


# w6=w5.withColumn('row_number1',row_number().over(WFF)).where('row_number==2')
# w6.show()





# #Basic CSV files 
# df = spark.read.format("csv").load(r"C:\Users\Ashok\Desktop\files\Match_Data.csv")
# df.show(3)

#csv with header 
df = spark.read.csv(r"C:\Users\Ashok\Desktop\files\Match_Data.csv",inferSchema=True,header=True) 
#df.show(3)
# df.printSchema()

# multiple options 
# df = spark.read.option("inferSchema",True).option("delimiter",",").csv(r"C:\Users\Ashok\Desktop\files\Match_Data.csv") 
# df.show(3)

#df.select('id','city','date','season').show()

#df = df.withColumn('date',df.date.cast('date'))

df = df.withColumn("date", to_date("date", "M/d/yyyy"))

# df.printSchema()

# df.show(2)


# WF=Window.partitionBy('season').orderBy(desc('total_count'))

# WF=Window.orderBy(desc('total_count'))

# df1=df.groupBy('season').agg(count(col('season')).alias('total_count')).withColumn('rank',rank().over(WF))

# df1.show()


#--Find the team that has won the most matches in a single season.
# WF = Window.orderBy(desc('total_count'))

# df2=df.groupBy('season','winner').agg(count('*').alias('total_count')).orderBy(desc('total_count')).withColumn('rank',dense_rank().over(WF)) #.where('rank=2')
# df2.show()


#--List all players who have won more than 5 "Player of the Match" awards.

# WF=Window.orderBy(desc('total_count'))
# df3=df.groupBy('player_of_match').agg(count('player_of_match').alias('total_count')).orderBy(desc('total_count')).withColumn('rank',dense_rank().over(WF)).where('rank =3')

# df3.show()

# WF=Window.partitionBy('season').orderBy(desc('total_count'))
# df3=df.groupBy('player_of_match','season').agg(count('player_of_match').alias('total_count')).orderBy(desc('total_count')).withColumn('rank',dense_rank().over(WF)) .where('rank <=2')

# df3.show()


#--Get the venues where more than 10 matches have been played.

# df4=df.groupby('venue').agg(count('venue').alias('total_match')).orderBy(desc('total_match')).where('total_match >=10')
# df4.show()



