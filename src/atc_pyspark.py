from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='drug').load('s3n://rxminer/drugbank/drugbank.xml')
df = df.select('name','products', 'atc-codes')
# explode to get "long" format
#df = df.withColumn('product', F.explode(df.products.product))
#str_schema = "atc_codes:struct<atc_code:array<struct<string,array<struct<string,string>>"
#df = df.select('name', col('atc-codes').cast(str_schema), 'product.ndc-product-code')
#df = df.withColumn('exploded', F.explode(df.atc_codes.atc-code))
# get the name and the name in separate columns
#df = df.withColumn('name', F.col('exploded').getItem(0))
#df = df.withColumn('value', F.col('exploded').getItem(1))
# now pivot
#df.groupby('Id').pivot('name').agg(F.max('value')).na.fill(0)

#df_rxevent = spark.read.csv('s3n://rxminer/SynPUFs/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_*.csv', header=True)
df.printSchema()
df.show(10)