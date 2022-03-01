import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

salesDF = glueContext.create_dynamic_frame.from_catalog(
             database="amitdbetl",
             table_name="sales")
customerDF = glueContext.create_dynamic_frame.from_catalog(
             database="amitdbetl",
             table_name="customers")

salesDF.printSchema()
customerDF.printSchema()

customersalesDF=Join.apply(salesDF, customerDF, 'customerid', 'customerid')
customersalesDF.printSchema()

customersalesDF = customersalesDF.drop_fields(['`.customerid`'])
customersalesDF.printSchema()

glueContext.write_dynamic_frame.from_options(customersalesDF, connection_type = "s3", connection_options = {"path": "s3://amit-data-lake/data/customersales"}, format = "json")


