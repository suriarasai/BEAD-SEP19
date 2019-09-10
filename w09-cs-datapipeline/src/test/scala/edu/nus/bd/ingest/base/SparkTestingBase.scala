package edu.nus.bd.ingest.base

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkTestingBase extends BeforeAndAfterAll {
  self: Suite =>

  @transient private var _spark: SparkSession = _

  implicit def spark: SparkSession = _spark

  val sparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    .set("spark.executor.extraJavaOptions", "--XX:+UseG1GC")
    .set("spark.scheduler.mode", "FAIR")
    .set("spark.sql.shuffle.partitions", "1")
    .set("spark.default.parallelism", "1")


  override def beforeAll() {
    _spark = SparkSession.builder()
      .appName("Test Context")
      .config(sparkConf)
      .master("local[*]")
      .getOrCreate()

    _spark.sparkContext.setLogLevel("ERROR")
  }
}
