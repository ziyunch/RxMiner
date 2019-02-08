from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode

spark = SparkSession.builder.getOrCreate()
#df = spark.read.format('xml').options(rowTag='drug').load('/home/ubuntu/RxMiner/src/drugbank.xml')
df_rxevent = spark.read.csv('s3n://rxminer/SynPUFs/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_*.csv', header=True)
#df.printSchema()
df_rxevent.show(10)