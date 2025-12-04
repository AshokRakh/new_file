import warnings
warnings.filterwarnings("ignore")


# from pyspark import SparkContext,SparkConf 

# conf = SparkConf().setAppName("RDD Creation")
# sc = SparkContext(conf=conf)

# data = [1, 2, 3, 4, 5]
# rdd = sc.parallelize(data)
# print(rdd.collect())

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()


# data = [("Ashok", 25), ("Ravi", 30)]
# df = spark.createDataFrame(data, ["Name", "Age"])
# df.show()

df = spark.read.csv(r"C:\\Users\\Ashok\\Desktop\\Files\\Match_Data.csv", header=True, inferSchema=True)

df.show() #----->  show data 

# df_json = spark.read.json("data.json")


# df_parquet = spark.read.parquet("data.parquet")

#df.show(5)   #-----> show first 5 row

#df.printSchema()  #----> show schema 

#df.columns    #-----> show columns names only


#print(f"Rows: {df.count()}, Columns: {len(df.columns)}")



#df.write.csv("output.csv", header=True)  #------> save csv file 

#df.write.parquet("output.parquet") #------> save paraquet file 


#df.write.json("output.json")  #----> save json file 


#df.write.saveAsTable("employee_data") #----> save table file 