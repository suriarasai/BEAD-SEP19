package edu.nus.bd.ingest

import edu.nus.bd.config.PipelineConfig.DataColumn
import edu.nus.bd.ingest.StageConstants._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

trait PipelineBase {

  implicit val spark = SparkSession
    .builder()
    .config(buildSparkConf())
    .appName("Boring Pipeline")
    .master("local[*]")
    .getOrCreate()

  //spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair_pool") //4

  def buildSparkConf(): SparkConf = new SparkConf()
    .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .set("spark.sql.warehouse.dir", "/tmp/awaywarehose")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    /*.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") //1
    .set("spark.kryoserializer.buffer.max", "1g")
    .set("spark.sql.autoBroadcastJoinThreshold", (10485760L * 5).toString)
    .set("spark.sql.shuffle.partitions", "1") //2
    .set("spark.default.parallelism", "1")
    .set("spark.scheduler.mode", "FAIR") //3
    .set("spark.scheduler.allocation.file", "/Users/arunma/IdeaProjects/SparkCSTemplate/src/main/resources/fair-scheduler.xml")*/

  def writeData(inputDf: DataFrame,
                outputFilePath: String,
                format: String,
                partitionCol: String,
                mode: SaveMode = SaveMode.Overwrite)
               (implicit spark: SparkSession): Unit = {
    inputDf
      .write
      .format(format)
      .mode(mode)
      .partitionBy(partitionCol)
      .save(outputFilePath)
  }

  def readData(inputFolderPath: String, format: String)
              (implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .format(format)
      .load(inputFolderPath)
  }


  def createEmptyDataFrameFromCols(columns: List[DataColumn])(implicit spark: SparkSession): DataFrame = {
    val schema = StructType(
      StructField(RowKey, StringType) ::
        columns.map(dataCol => StructField(dataCol.name, CatalystSqlParser.parseDataType(dataCol.dType), dataCol.nullable))
    )
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }


  def getPrimaryKeyColumns(columns: List[DataColumn]): List[DataColumn] = columns.filter(_.primaryKey)

  def figureOutLastPartition(partition: String, days: Int = 10, pattern: String = "yyyy_MM_dd"): List[String] = {
    val fmt = DateTimeFormat.forPattern(pattern)
    val todaysPartition = DateTime.parse(partition, fmt)
    val previousDays = (1 to days).map(todaysPartition.minusDays).map(_.toString(pattern))
    previousDays.toList
  }


  def getLastPartition(rootPath: String, partition: String, processedDf: DataFrame, columns: List[DataColumn])(implicit spark: SparkSession): DataFrame = {
    val previousPossiblePartitions = figureOutLastPartition(partition)
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val previousDayOpt = previousPossiblePartitions.find(day => fileSystem.exists(new Path(s"$rootPath/$PartitionColumn=$day")))
    previousDayOpt.map(prev => spark.read.format("avro").load(s"$rootPath/$PartitionColumn=$prev")).getOrElse(createEmptyDataFrameFromCols(columns))
  }


}
