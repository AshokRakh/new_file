from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

spark = SparkSession.builder.appName("adidas project").getOrCreate()

df = spark.read.csv(r"C:\Users\Ashok\Desktop\files\adidas1.csv", header=True, inferSchema=True)

# df.show(5)

df = df.withColumn("Total Sales", regexp_replace(col("Total Sales"), ",", "").cast("double")) \
       .withColumn("operating profit", regexp_replace(col("operating profit"), ",", "").cast("double")) \
       .withColumn("price per unit", regexp_replace(col("price per unit"), ",", "").cast("double")) \
       .withColumn("units sold", regexp_replace(col("units sold"), ",", "").cast("double"))




total_rows = df.count()
distinct_rows = df.distinct().count()

print("Total rows:", total_rows)
print("Distinct rows:", distinct_rows)
print("Duplicate rows:", total_rows - distinct_rows)

df.createOrReplaceTempView("ashok")

spark.sql("select * from ashok").show()

spark.sql("select sum(`Total Sales`) as total_sales,sum(`operating profit`) total_profit,avg(`price per unit`) av_price_per_unit,sum(`units sold`) total_unit_sold from ashok").show()

# spark.sql("""
#     SELECT 
#         SUM(`Total Sales`) AS total_sales,
#         SUM(`operating profit`) AS total_profit,
#         AVG(`price per unit`) AS av_price_per_unit,
#         SUM(`units sold`) AS total_unit_sold
#     FROM ashok
# """).show()


#2:total sales by month

spark.sql("""
    SELECT 
        SUM(`total sales`) AS total_sales,
        date_format(to_date(`Invoice Date`, 'M/d/yyyy'), 'MMMM') AS month 
    FROM ashok 
    GROUP BY 2
""").show()

#3:total sales by state

spark.sql("select sum(`total sales`) total_sales,state  from  ashok group by 2 order by 1 desc limit 5").show()

#4:total sales by region
spark.sql("select sum(`total sales`) total_sales,region  from ashok group by 2").show()

#5:total sales by product

spark.sql("""select sum(`total sales`) total_sales,Product
  from ashok
group by 2
order by 1 desc limit 5""").show()

#6:total sales by retailer


spark.sql("""select sum(`total sales`) total_sales,retailer
  from ashok
group by 2
order by 1 desc limit 5""").show()

#7:unit sold by category

spark.sql("""select sum(`Units Sold`) total_unit_sold,`sales method`
             from ashok
            group by 2""").show()


#8:top performing cities by profit

spark.sql("""select sum(`operating profit`) total_profit,city
  from ashok
group by 2
order by 1 desc limit 5""").show()