package edu.nus.bd.ingest


import cats.syntax.either._
import edu.nus.bd.config.PipelineConfig
import edu.nus.bd.config.PipelineConfig.{DatasetConfig, Delimited}
import edu.nus.bd.ingest.StageConstants._
import edu.nus.bd.ingest.models.ErrorModels.DataError
import edu.nus.bd.ingest.stages.{AddRowKeyStage, DataTypeCastStage, DataTypeValidatorStage}
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object DataStagingPipelineMain extends PipelineBase {

  def main(args: Array[String]): Unit = {
    val filePath = args(0)

    LogManager.getRootLogger.setLevel(Level.WARN)
    spark.sparkContext.setLogLevel("ERROR")

    val fileName = StringUtils.substringAfterLast(filePath, "/")
    val dataSetE =
      for {
        ingestionConfig <- PipelineConfig.getIngestionConfig(getClass.getResource("/datasets_main.conf"), "ingestionConfig")
        dataSetIn <- PipelineConfig.findDatasetFromFileName(fileName, ingestionConfig)
      } yield dataSetIn

    if (dataSetE.isLeft) {
      sys.error(s"No matching dataset configuration found for the given file. Please check the filePattern configuration for each dataset. Error : ${dataSetE.left.toString}")
    }
    dataSetE.map(config => runPipeline(filePath, config))
  }

  private def runPipeline(filePath: String, dataSetConfig: DatasetConfig)(implicit spark: SparkSession) = {
    val sourceRawDf =
      spark
        .read
        .format("csv")
        .option("header", true)
        .option("delimiter", dataSetConfig.fileFormat.asInstanceOf[Delimited].delimiter)
        .load(filePath)
        .cache()

    import DataFrameOps._

    val dataCols = dataSetConfig.columns
    val dataPath = s"${dataSetConfig.dataPath}/${dataSetConfig.name}"
    val errPath = s"${dataSetConfig.errorPath}/${dataSetConfig.name}"
    val primaryKeyCols = getPrimaryKeyColumns(dataSetConfig.columns)

    val pipelineStages = List(
      new AddRowKeyStage(dataCols),
      new DataTypeValidatorStage(dataCols),
      new DataTypeCastStage(dataCols)
    )
    val init = (spark.emptyDataset[DataError], sourceRawDf)

    val (errors, processedDf) = pipelineStages.foldLeft(init) { case ((accumErr, df), stage) =>
      stage(accumErr, df)
    }

    val partition = StringUtils.substringBeforeLast(StringUtils.substringAfterLast(filePath, "-"), ".")

    val dataDf =
      if (dataSetConfig.dataShape == "Dimension") {
        val previousDf = getLastPartition(dataPath, partition, processedDf, dataSetConfig.columns)
        processedDf.union(previousDf).dropDuplicates(primaryKeyCols.map(_.name))
      }
      else {
        processedDf
      }

    writeData(dataDf.withColumn(PartitionColumn, lit(partition)), dataPath, "avro", PartitionColumn, SaveMode.Append)
    writeData(errors.withColumn(PartitionColumn, lit(partition)), errPath, "avro", PartitionColumn, SaveMode.Append)

    dataDf.show(false)
    errors.show(false)

    dataDf.printSchema()

    println(s"processed Df count : ${dataDf.count()}")

    Thread.sleep(1000 * 60 * 10)
  }
}
