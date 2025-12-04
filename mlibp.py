
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
import os

spark = SparkSession.builder \
    .appName("PySpark_MLlib_Projects") \
    .master("local[*]") \
    .getOrCreate()

base_path = os.path.join(os.getcwd(), "pyspark_mllib_package")

# -------------------------
# 1) Classification - Customer Churn Prediction (Logistic Regression)
# -------------------------
print("\n=== Classification: Customer Churn Prediction ===")

df_churn = spark.read.csv(r"C:\\Users\\Ashok\\Desktop\\mlib\\customer_churn_large.csv",header=True, inferSchema=True)

#categorical label index 
indexer = StringIndexer(inputCol="Churn", outputCol="label")
df_churn = indexer.fit(df_churn).transform(df_churn)

# features
assembler = VectorAssembler(inputCols=["Age", "Balance", "NumOfProducts", "EstimatedSalary"], outputCol="rawFeatures")
df_churn = assembler.transform(df_churn)

# feature scaling 
scaler = StandardScaler(inputCol="rawFeatures", outputCol="features")
scaler_model = scaler.fit(df_churn)
df_churn = scaler_model.transform(df_churn).select("features", "label")

# train-test split
train_churn, test_churn = df_churn.randomSplit([0.8, 0.2], seed=42)

# Logistic Regression 
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20)
lr_model = lr.fit(train_churn)
predictions_churn = lr_model.transform(test_churn)

# Evaluate
evaluator_clf = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator_clf.evaluate(predictions_churn)
print(f"Classification Accuracy: {accuracy:.4f}")

# -------------------------
# 2) Regression - House Price Prediction (Linear Regression)
# -------------------------
print("\n=== Regression: House Price Prediction ===")
df_house = spark.read.csv(r"C:\Users\Ashok\Desktop\mlib\house_price_large.csv",header=True,inferSchema=True)


assembler_reg = VectorAssembler(inputCols=["sqft", "bedrooms", "bathrooms", "floors"], outputCol="features")
df_house = assembler_reg.transform(df_house).select("features", "price")

train_house, test_house = df_house.randomSplit([0.8, 0.2], seed=42)

lr_reg = LinearRegression(featuresCol="features", labelCol="price", maxIter=50)
lr_reg_model = lr_reg.fit(train_house)
predictions_house = lr_reg_model.transform(test_house)
predictions_house.select("price", "prediction").show(5)

# Evaluate regression
evaluator_reg = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")
r2 = evaluator_reg.evaluate(predictions_house)
rmse = evaluator_reg.evaluate(predictions_house, {evaluator_reg.metricName: "rmse"})
print(f"Regression R2: {r2:.4f}, RMSE: {rmse:.2f}")

# -------------------------
# 3) Clustering - Customer Segmentation (KMeans)
# -------------------------
print("\n=== Clustering: Customer Segmentation (KMeans) ===")
df_cust = spark.read.csv(r"C:\Users\Ashok\Desktop\mlib\customer_data_large.csv",header=True, inferSchema=True)


assembler_clust = VectorAssembler(inputCols=["Age", "Income", "SpendingScore"], outputCol="features")
df_cust = assembler_clust.transform(df_cust).select("CustomerID", "features")

k = 3
kmeans = KMeans(k=k, seed=42, featuresCol="features")
kmeans_model = kmeans.fit(df_cust)
clustered = kmeans_model.transform(df_cust)
clustered.show(truncate=False)

print("Cluster Centers:")
for center in kmeans_model.clusterCenters():
    print(center)


spark.stop()
