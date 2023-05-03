from pyspark.sql import SparkSession
import mysql.connector 
from pyspark.sql.functions import to_date
import pandas as pd 
spark = SparkSession.builder.appName("mysql-etl").getOrCreate()

def readInputData(file_type,table_name):
    if file_type=='parquet':
        return spark.read.parquet("gs://spark-result-bkt/"+table_name+"/")
    elif file_type=='csv':
        return spark.read.csv("gs://pyspark-fs-sid/"+table_name+"/",header=True)

def createTempTables(df,table_name):
    df.createOrReplaceTempView(table_name)

def writeOutputSink(df,mode,output_folder_name,partition_column=None):
    if partition_column is not None:
        df.write \
            .partitionBy(partition_column) \
            .mode(mode) \
            .parquet("gs://spark-result-bkt/"+output_folder_name+"/")
    else :
        df.write \
            .mode(mode) \
            .parquet("gs://spark-result-bkt/"+output_folder_name+"/")

df_orders = readInputData("parquet","orders")
df_order_items = readInputData("parquet","order_items")
df_products = readInputData("csv","products")

createTempTables(df_orders,"orders")
createTempTables(df_order_items,"order_items")
createTempTables(df_products,"products")

# Part-1 | Order Status Summary
sql = """
    select
        count(distinct order_id) as total_orders,
        round(sum(num_items),1) as total_ordered_items,
        round(avg(num_items),1) as avg_items_per_order,
        date(created_at) as order_date,
        status 
    from 
        orders 
    where 
        date(created_at)>='2023-03-25'
    group by 
        4,5
    order by order_date
    """

df_order_status_summary = spark.sql(sql)
writeOutputSink(df_order_status_summary,"overwrite","order_status_summary","order_date")

# Join order_items and products table
sql = """
    select 
        oi.order_id,
        date(oi.created_at) as order_date,
        p.id as product_id,
        p.category as product_category,
        p.brand as product_brand ,
        p.name as product_name,
        p.department as product_dept,
        round(p.retail_price) as retail_price,
        oi.sales_price
    from 
        products p
    inner join 
        order_items oi 
    on 
        p.id = oi.product_id 
     """

df_item_product = spark.sql(sql)
createTempTables(df_item_product,"item_products")

# Part-2 | Get the top 3 Products per brand ranked by total sales count for the month of March 
# Use the table item_products created in the previous step 
qry = """
    with first_level_aggregation as (
        select 
            product_id,
            product_name,
            product_category,
            order_date,
            count(1) num_times_sold
        from 
            item_products
        where 
            order_date between '2023-03-01' and '2023-03-31'
        group by 
            1,2,3,4
    ) ,
    ranked_products as (
        select 
            product_name,
            product_category,
            order_date,
            row_number() over (partition by order_date order by num_times_sold desc) as rank
        from 
            first_level_aggregation 
        order by 
            3,4
    )
    select 
        order_date,
        product_name,
        product_category,
        rank
    from 
        ranked_products
    where 
        rank<=3 
    order by 
        order_date,rank
    """

top3_ranked_products = spark.sql(qry)
writeOutputSink(top3_ranked_products,"overwrite","top3_ranked_products")

# Part-3 | Get the top 3 Products per brand
sql = """
    with first_level_aggregation as (
        select 
            product_brand, 
            extract(month from order_date) as sales_month,
            sum(sales_price) as sales_amount ,
            count(distinct order_id) as num_of_orders
        from 
            item_products
        group by 
            1,2
        order by 
            2
    )
    select 
        product_brand,
        sales_month,
        sales_amount,
        sum(sales_amount) 
            over (
                    partition by product_brand
                    order by sales_month
                    rows between unbounded preceding and current row
                ) as running_total_sales_amt,
        sum(num_of_orders) 
            over (
                    partition by product_brand
                    order by sales_month
                    rows between unbounded preceding and current row
                ) as running_total_order_count
    from 
        first_level_aggregation
    where 
        1=1 
        and product_brand='AG Jeans' 
    order by 
        sales_month,
        product_brand
    """

running_sales_by_brand = spark.sql(sql)
writeOutputSink(running_sales_by_brand,"overwrite","running_sales_by_brand")