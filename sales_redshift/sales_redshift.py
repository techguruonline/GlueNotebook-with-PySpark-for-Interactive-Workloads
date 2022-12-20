import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
my_conn_options = {  
    "url": "jdbc:redshift://redshift-spark-integration.c8qeuccnncmq.us-east-1.redshift.amazonaws.com:5439/retail",
    "dbtable": "sales",
    "user": "awsuser",
    "password": "Redshift-Spark2022",
    "redshiftTmpDir": "s3://redshift-spark-integration/tmp/",
    "aws_iam_role": "arn:aws:iam::484830312924:role/Redshift-Spark_integration"
}

salesDyF = glueContext.create_dynamic_frame_from_options("redshift", my_conn_options)
salesDyF.show(10)
my_conn_options = {  
    "url": "jdbc:redshift://redshift-spark-integration.c8qeuccnncmq.us-east-1.redshift.amazonaws.com:5439/retail",
    "dbtable": "product",
    "user": "awsuser",
    "password": "Redshift-Spark2022",
    "redshiftTmpDir": "s3://redshift-spark-integration/tmp/",
    "aws_iam_role": "arn:aws:iam::484830312924:role/Redshift-Spark_integration"
}

productDyF = glueContext.create_dynamic_frame_from_options("redshift", my_conn_options)
productDyF.show(10)
my_conn_options = {  
    "url": "jdbc:redshift://redshift-spark-integration.c8qeuccnncmq.us-east-1.redshift.amazonaws.com:5439/retail",
    "dbtable": "customer",
    "user": "awsuser",
    "password": "Redshift-Spark2022",
    "redshiftTmpDir": "s3://redshift-spark-integration/tmp/",
    "aws_iam_role": "arn:aws:iam::484830312924:role/Redshift-Spark_integration"
}

customerDyF = glueContext.create_dynamic_frame_from_options("redshift", my_conn_options)
customerDyF.show(10)
# We will perform just the join and skip the rest of the DynamicFrame operations, please refer to the other Notebook "AWS Glue Notebook with PySpark.ipynb" if you want to explore DynamicFrame functions on these datasets

joinedDyF = Join.apply(customerDyF, Join.apply(salesDyF, productDyF, "product_id", "product_id"), "customer_id", "customer_id")
# As the joined dyanmic frame will have wider columns from all the 3 dynamic frames, we will use a DynamicFrame class called "select_fields" to display only a set of selective fileds and also conver to a DataFrame

selectColumnsDF = joinedDyF.select_fields(paths=["customer_id", "first_name", "last_name", "product_id", "product_desc", "quantity", "unit_price", "country", "invoice_no"]).toDF()
# Add a new derived column to the DataFrame, let's call the new dervied column as "total_sales" which is derived by multipying quantity with unit_price for each transaction
from pyspark.sql.functions import col

derivedColDF = selectColumnsDF.withColumn("total_sales", col("quantity").cast("Integer") * col("unit_price").cast("Float"))
derivedColDF.show(10)
# We will explore Spark SQL on this newly created DataFrame, for that we will have create  a temp veiw

derivedColDF.createOrReplaceTempView("salesDF")
# Using Spark SQL select data from the temprory view which is nothing but teh DataFrame that we created in the previous steps
# Note that with Spark SQL, we are writing the ANSI SQL that we use in the traditional databases

spark.sql("SELECT * FROM salesDF LIMIT 10").show(truncate=False)
# Where clause to filter data that belongs to Spain

spark.sql("SELECT * FROM salesDF WHERE country = 'Spain' LIMIT 10").show(truncate=False)
# Spark provides powerful functions using which we can efficiently perform complex data analytics like aggregations, joins etc
# With the DataFrame, we will perform data analysis by using aggregate function to see total sales by Customer, total sales by Customer by Product, total revenue by country.
# We will use spark SQL to perform these aggregate functions

#Total sales by Customer, alias aggregate column to a new name, order sum_sales descending with the highest sales on the top. Round sum_sales to 2 decimal places

spark.sql("SELECT customer_id, round(sum(total_sales),2) as sum_sales FROM salesDF GROUP BY customer_id ORDER BY sum_sales DESC").show(truncate=False)

# Total sales by Customer by Product order by sum_sales with the highest sale on the top. Round sum_sales to 2 decimal places

spark.sql("SELECT customer_id, product_id, round(sum(total_sales),2) as sum_sales FROM salesDF GROUP BY customer_id, product_id ORDER BY sum_sales DESC").show(truncate=False)
# Total sales by Country

spark.sql("SELECT country, round(sum(total_sales),2) as sum_sales FROM salesDF GROUP BY country ORDER BY sum_sales DESC").show(truncate=False)
# Total sales by customer, Product, Country

spark.sql("SELECT customer_id, product_id, country, round(sum(total_sales),2) as sum_sales FROM salesDF GROUP BY customer_id, product_id, country ORDER BY sum_sales DESC").show(truncate=False)
# For simplicity and to write to Glue Data Catalog, we can convert the Spark DataFrame back to Glue DynamicFrame and write to either S3 directly or to Catalog
# Convert back to DynamicFrame
from awsglue.dynamicframe import DynamicFrame

derivedColDyF = DynamicFrame.fromDF(derivedColDF,glueContext, "convert")
my_conn_options = {
    "dbtable": "sales_tgt",
    "database": "retail",
    "aws_iam_role": "arn:aws:iam::484830312924:role/Redshift-Spark_integration"
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = derivedColDyF, 
    catalog_connection = "Redshift-Direct", 
    connection_options = my_conn_options, 
    redshift_tmp_dir = "s3://redshift-spark-integration/tmp/")
job.commit()