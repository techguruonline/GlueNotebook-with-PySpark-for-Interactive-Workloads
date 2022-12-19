#Import libraries, create Spark and Glue context, initiate spark session

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
# Create DynamicFrame for Customer dataset
customer = glueContext.create_dynamic_frame.from_catalog(
    database = "retail_s3", 
    table_name = "customer")
# Print number of records in the DynamicFrame and print schema
print("Record Count: ", customer.count())
customer.printSchema()
# View records from Customer dynamicframe
customer.show(10)
# Create DynamicFrame for Sales dataset
sales = glueContext.create_dynamic_frame.from_catalog(
    database = "retail_s3",
    table_name = "sales")
# Print number of records in the DynamicFrame and print schema
print("Record Count: ", sales.count())
sales.printSchema()
# View records from Sales dynamicframe
sales.show(10)
# Create DynamicFrame for Product dataset
product = glueContext.create_dynamic_frame.from_catalog(
    database = "retail_s3",
    table_name = "product")
# Print number of records in the DynamicFrame and print schema
print("Record Count: ", product.count())
product.printSchema()
# View records from Product dynamicframe
product.show(10)
# Now that we have all the 3 source data files loaded into memory as part of creating the dynamic frame
# Let's join all the 3 dynamic frames to get the combined view
# DynamicFrame provides various classes like filter, join, map, count etc which we can use for our data transformations.
# For more details, please refer to the Glue Developer Guide doc -> https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame.html#aws-glue-api-crawler-pyspark-extensions-dynamic-frame-toDF

joinedDyF = Join.apply(customer, Join.apply(sales, product, "product_id", "product_id"), "customer_id", "customer_id")
# as the joined dyanmic frame will have wider columns from all the 3 dynamic frames, we will use a DynamicFrame class called "select_fields" to display only a set of selective fileds

selectColumnsDyF = joinedDyF.select_fields(paths=["customer_id", "first_name", "last_name", "product_id", "product_desc", "quantity", "unit_price", "country", "invoice_no"])  
# Let's view the data and count
selectColumnsDyF.show(10)

print("Record count: ", selectColumnsDyF.count())
# Now let's look at filtering the data, get the records which have country = 'United Kingdom"
filteredDyF = Filter.apply(frame = selectColumnsDyF,
                           f = lambda x: x["country"] in "United Kingdom"
                          )
# Let's view the data and count
filteredDyF.show(10)

print("Record count: ", filteredDyF.count())
# Drop field invoice_no
dropFieldsDyF = filteredDyF.drop_fields(["invoice_no"])
dropFieldsDyF.show(10)
# Rename column / Change Field name: quantity to qty
dropFieldsDyF.rename_field("quantity", "qty").show(10)
# Another way to remane a column is by applying mapping similar to the Glue Studio -> Actions -> ApplyMappings
# With mapping, we can also change the datatypes
mapping = [("first_name","string","first_name","string"),("last_name","string","last_name","string"),("product_id","string","product_id","string"),("product_desc","string","product_desc","string"),("unit_price","double","unit_price","double"),
           ("customer_id","long","customer_id","long"),("country","string","country","string"),("quantity","long","qty","long")]

mapDyF = ApplyMapping.apply(
    frame = dropFieldsDyF,
    mappings = mapping
)

mapDyF.show(10)
# Write data to S3
glueContext.write_dynamic_frame.from_options(
    frame = mapDyF,
    connection_type = "s3",
    connection_options = {"path":"s3://redshift-spark-integration/data/tgt/"},
    format = "parquet"
)
# Now Let's conver the DynamicFrame to a DataFrame
salesDF = selectColumnsDyF.toDF()
# Print Schema of the Spark DataFrame
salesDF.printSchema()
# Select columns from the DataFrame, unlike Glue DynamicFrame, the output of a DataFrame is in tabular format
salesDF.select("customer_id","product_id","country").show()
# Let's try adding, dropping, renaming columns and also perform some aggregate functions on the dataframe
# We need to import additional libraries

from pyspark.sql.functions import col
# Add a new derived column to the DataFrame, let's call the new dervied column as "total_sales" which is derived by multipying quantity with unit_price for each transaction

DerivedColDF = salesDF.withColumn("total_sales", col("quantity").cast("Integer") * col("unit_price").cast("Float"))
DerivedColDF.show(10)
#drop invoice number column from the DataFrame, am not storing this to a new variable, rather just showing how its done with Spark DataFrame
DerivedColDF.drop("invoice_no").show(10)
# Rename an existing column to a new name, am not storing this to a new variable, rather just showing how its done with Spark DataFrame
DerivedColDF.withColumnRenamed("quantity","qty").show(10)
# Spark provides powerful functions using which we can efficiently perform complex data analytics like aggregations, joins etc
# With the sales DataFrame we have, we will perform data analysis by using aggregate function to see total sales by Customer, total sales by Customer by Product, total revenue by country.

#Total sales by Customer, renamed aggregate column to a new name 
totalSalesbyCustDF = DerivedColDF.groupBy("customer_id").sum("total_sales").withColumnRenamed("sum(total_sales)", "sum_sales")
totalSalesbyCustDF.show(10)
# Total sales by Customer by Product

totalSalesbyCustProdDF = DerivedColDF.groupBy("customer_id", "product_id").sum("total_sales").withColumnRenamed("sum(total_sales)", "sum_sales")
totalSalesbyCustProdDF.show(10)
# Total sales by Country

totalSalesbyCountryDF = DerivedColDF.groupBy("country").sum("total_sales").withColumnRenamed("sum(total_sales)", "sum_sales")
totalSalesbyCountryDF.show(10)
# Now let's filter the data from DataFrame, this is much straight forward to use where clause as with teh traditional SQL

#Filter data by country = "United Kingdom"
filteredDF = DerivedColDF.where("country == 'United Kingdom'")
filteredDF.show(10)
# First option to persist data to S3

# Write dataframe directly to S3 as a Parquet file

DerivedColDF.write.parquet("s3://redshift-spark-integration/data/tgt/data")
# For simplicity and to write to Glue Data Catalog, we can convert the Spark DataFrame back to Glue DynamicFrame and write to either S3 directly or to Catalog

# Conver back to DynamicFrame
from awsglue.dynamicframe import DynamicFrame

DerivedColDyF = DynamicFrame.fromDF(DerivedColDF,glueContext, "convert")
# Write data to S3 using DynamicFrame

glueContext.write_dynamic_frame_from_options(
    frame=DerivedColDyF,
    connection_type = "s3",
    connection_options = {"path":"s3://redshift-spark-integration/data/tgt/"},
    format = "parquet")
glueContext.write_dynamic_frame.from_catalog(
    frame=DerivedColDyF,
    database = "retail_s3",
    table_name = "tgt")
job.commit()