from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='drug').load('s3n://rxminer/drugbank/drugbank.xml')
df.show(10)