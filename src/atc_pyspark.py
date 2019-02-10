from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType, LongType, StringType, StructField, StructType)

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='drug').load('s3n://rxminer/drugbank/drugbank.xml')
df = df.select('name','products', 'atc-codes')
# explode to get "long" format
df = df.withColumn('product', F.explode(df.products.product))
df = df.withColumnRenamed("atc-codes", "atccodes")
str_schema = "struct<atccode:array<struct<string,array<struct<string,string>>>>>"
df.select(F.col('atccodes').cast(str_schema)).printSchema()
df = df.withColumn('exploded', F.explode(df.atccodes.atccode))
# get the name and the name in separate columns
df = df.withColumn('name', F.col('exploded').getItem(0))
df = df.withColumn('value', F.col('exploded').getItem(1))
# now pivot
df.groupby('Id').pivot('name').agg(F.max('value')).na.fill(0)

#df_rxevent = spark.read.csv('s3n://rxminer/SynPUFs/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_*.csv', header=True)
df.printSchema()
df.show(10)