package edu.nus.bd.ingest

import cats.syntax.either._
import edu.nus.bd.config.PipelineConfig
import edu.nus.bd.config.PipelineConfig.IngestionConfig
import edu.nus.bd.ingest.StageConstants._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object DataSummaryMain extends PipelineBase {

  def main(args: Array[String]): Unit = {
    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel("ERROR")

    val outputPath = args(0)
    val partition = args(1)
    runStats(PipelineConfig.getIngestionConfig(getClass.getResource("/datasets_main.conf"), "ingestionConfig").toOption.get, outputPath, partition)
  }

  private def runStats(ingestionConfig: IngestionConfig, outputPath: String, partition: String)(implicit spark: SparkSession): Unit = {
    val partitionPathForDemo = PipelineConfig.findDatasetFromFileName("demographics", ingestionConfig).toOption.get.dataPath + s"/demographics/$PartitionColumn=$partition"
    val partitionPathForTxn = PipelineConfig.findDatasetFromFileName("transactions", ingestionConfig).toOption.get.dataPath + s"/transactions/$PartitionColumn=$partition"
    val demoDf = readData(partitionPathForDemo, "avro")
    val txnDf = readData(partitionPathForTxn, "avro")

    demoDf
      .join(txnDf, "accountID")
      .groupBy("accountID")
      .sum("transactionAmountUSD")
      .withColumnRenamed("sum(transactionAmountUSD)", "TransactionAmount")
      .describe("accountID", "TransactionAmount")
      .show(10000, false)
  }


}

case class HistRow(startPoint:Double,count:Long)
