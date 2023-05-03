from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,DecimalType

spark = SparkSession.builder \
    .appName("Product Recommendation") \
    .getOrCreate()

users_df = spark.read.csv("gs://bucket-name/collabertive_filtering_inputdata/users.csv",header=True)
products_df = spark.read.csv("gs://bucket-name/collabertive_filtering_inputdata/products.csv",header=True)
order_items_df = spark.read.csv("gs://bucket-name/collabertive_filtering_inputdata/order_items.csv",header=True)


user_indexer = StringIndexer(inputCol="id", outputCol="user_id_index")
product_indexer = StringIndexer(inputCol="id", outputCol="product_id_index")

users_df = user_indexer.fit(users_df).transform(users_df)
products_df = product_indexer.fit(products_df).transform(products_df)

(training_df, test_df) = order_items_df.randomSplit([0.8, 0.2])

training_df = training_df \
                .withColumn("user_id",training_df.user_id.cast(IntegerType())) \
                .withColumn("product_id",training_df.product_id.cast(IntegerType())) \
                .withColumn("sale_price",training_df.sale_price.cast(DecimalType()))

# training_df.show(10,False)

als = ALS(userCol="user_id", itemCol="product_id", ratingCol="sale_price", coldStartStrategy="drop")

model = als.fit(training_df)
model.write().overwrite().save("gs://bucket-name/collabertive_filtering_artifact")