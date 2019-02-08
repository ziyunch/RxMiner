from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='drug').load('s3n://rxminer/drugbank/drugbank/drugbank.xml')
df.printSchema()
df.show(10)