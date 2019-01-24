import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra

object price_data {
  def main(args: Array[String]) {

    // setup the Spark Context named sc
    val conf = new SparkConf().setAppName("SynPUFs Processor")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()
    val df = spark.read.option("header", true).csv('s3n://rxminer/SynPUFs/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.csv')

    df.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "tablename", "keyspace" -> "keyspace")).save()
  }
}