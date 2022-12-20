# GlueNotebook-with-PySpark-for-Interactive-Workloads
Demonstrate how to use Glue Notebook with PySpark for interactive workloads. Extraction, Transformation and loading with Glue DynamicFrame, seamlessly convert DynamicFrame to a Spark DataFrame, continue to transform data using DataFrame functions and convert back to DynamicFrame to continue with teh ETL or to write / persist data directly to S3 or through the Glue Catalog.

Files & Folder Structure:

sales folder: sales.py - This folder contains the Python code which was generated automatically by AWS Glue based on the code that I wrote on the Glue Notebook. This code if for Glue Notebook, Pyspark and S3.
        sales.json -  Also created automatically by AWS Glue, this file containes Glue config.
        
sales_redshift: sales_redshift.py - This folder contains the Python code which was generated automatically by AWS Glue based on the code that I wrote on the Glue Notebook. This code is for Glue Notebook, Pyspark and Redshift.
        sales_redshift.json -  Also created automatically by AWS Glue, this file containes Glue config.
        
AWS Glue Notebook with PySpark.ipynb: Jupyter Notebook that you can use download and use for learning purposes in your own AWS account. This Notebook is for Glue Notebook, Pyspark and S3.

AWS Glue Notebook with PySpark and Redshift.ipynb: Jupyter Notebook that you can use download and use for learning purposes in your own AWS account. This Notebook is for Glue Notebook, Pyspark and Redshift.

source-data: This folder contains a compressed zip file. Within teh zip file you will find 3 source data files, customer.csv, product_details.csv and sales.csv Please download the zip and use it along with the Notebook to follow along the steps.
