from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='drug').load('s3n://rxminer/drugbank/drugbank.xml')
df = df.select('name','products', 'atc-codes')
df = df.withColumn('product', explode(df.products.product))
df = df.select('name', 'atc-codes.element._code', 'atc-codes.element.level','product.ndc-product-code')

#df_rxevent = spark.read.csv('s3n://rxminer/SynPUFs/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_*.csv', header=True)
df.printSchema()
df.show(100)