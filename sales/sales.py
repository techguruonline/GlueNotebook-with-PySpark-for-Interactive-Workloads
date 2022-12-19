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

selectiveDyF = joinedDyF.select_fields(paths=["customer_id", "first_name", "last_name", "product_id", "product_desc", "quantity", "unit_price", "country", "invoice_no"])  
# Let's view the data and count
selectiveDyF.show(10)

print("Record count: ", selectiveDyF.count())
# Now let's look at filtering the data, get the records which have country = 'United Kingdom"
filteredDyF = Filter.apply(frame = selectiveDyF,
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
selectiveDF = selectiveDyF.toDF()
job.commit()