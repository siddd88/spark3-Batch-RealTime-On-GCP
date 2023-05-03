from pyspark.sql import SparkSession
import mysql.connector
import pandas as pd
from pyspark.sql.functions import to_date
spark = SparkSession.builder.appName("pyspark-mysql-extraction").getOrCreate()

def connectMysql():
    connection = mysql.connector.connect(
        user='root',
        password='Udemy123',
        database='ecommerce_db',
        host='10.32.16.6',
        port='3306'
    )
    return connection

def closeMysqlConnection(connection):
    connection.close()

def extractData(sql_qry,connection):
    return pd.read_sql(sql_qry,con=connection)

def convertPandastoSparkdf(pandas_df):
    return spark.createDataFrame(pandas_df)

def dt_transformation(df):
    return df.withColumn("partition_date",to_date(df.created_at))

def writeOutputSink(df,partition_column,table_name,mode):
    df.write.partitionBy(partition_column).mode(mode).parquet("gs://spark-result-bkt/"+table_name+"/")

connection = connectMysql()

qry_orders = "select * from orders"
qry_order_items = "select * from order_items"

pd_orders = extractData(qry_orders,connection)
pd_order_items = extractData(qry_order_items,connection)

closeMysqlConnection(connection)

df_orders = convertPandastoSparkdf(pd_orders)
df_order_items = convertPandastoSparkdf(pd_order_items)

df_orders = dt_transformation(df_orders)
df_order_items = dt_transformation(df_order_items)

writeOutputSink(df_order_items,"partition_date","order_items","overwrite")
writeOutputSink(df_orders,"partition_date","orders","overwrite")