from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName("Product Recommendation").getOrCreate()

model = ALSModel.load("gs://bucket-name/product_recommendations_model")

single_user = 34688 # user id of the user you want to get recommendations for
single_user_df = spark.createDataFrame([(single_user,)], ["user_id"])
user_recs = model.recommendForUserSubset(single_user_df, 10) # get top 10 recommendations for the user

user_recs.show(truncate=False)