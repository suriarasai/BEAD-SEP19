package edu.nus.bd.config

import java.net.URL

import cats.syntax.either._
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig._
// @formatter:off

object PipelineConfig {

  case class IngestionConfig(global: GlobalConfigs, datasets: List[DatasetConfig])

  case class GlobalConfigs()

  //Dataset
  sealed trait DatasetConfig {
    def name: String
    def filePattern: String
    def fileFormat: FileFormat
    def dataPath: String
    def errorPath: String
    def dataShape: String
    def columns: List[DataColumn]
  }

  case class BatchSourceDatasetConfig(name: String,
                                      filePattern: String,
                                      fileFormat: FileFormat,
                                      dataPath: String,
                                      errorPath: String,
                                      dataShape: String,
                                      columns: List[DataColumn]
                                     ) extends DatasetConfig

  case class FixedWidthSourceDatasetConfig(name: String,
                                           filePattern: String,
                                           fileFormat: FileFormat,
                                           dataPath: String,
                                           errorPath: String,
                                           dataShape: String,
                                           columns: List[DataColumn]
                                          ) extends DatasetConfig


  //File formats
  sealed trait FileFormat

  case class Delimited(delimiter: String) extends FileFormat

  case class FixedWidth(lenConfigs: List[LengthConfig]) extends FileFormat

  case class LengthConfig(name: String, length: String)

  //Stage Config
  sealed trait StageConfig

  case class AddRowKeyStageConfig(params: Option[Config]) extends StageConfig

  case class WriteToHBaseStageConfig(params: Option[Config]) extends StageConfig

  case class ReplaceCharDataStageConfig(params: Option[Config]) extends StageConfig

  case class DataTypeValidatorStageConfig(params: Option[Config]) extends StageConfig

  case class DataTypeCastStageConfig(params: Option[Config]) extends StageConfig

  //Columns
  case class DataColumn(name: String, dType: String, format: String = "", nullable: Boolean = true, primaryKey: Boolean = false)


  def get(config: Config, rootKey: String): IngestionConfig = {

    implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

    implicit val dataSetSourceTypeConfHint = new FieldCoproductHint[DatasetConfig]("sourceType") {
      override def fieldValue(name: String) = name
    }

    implicit val fileFormatTypeConfHint = new FieldCoproductHint[FileFormat]("fType") {
      override def fieldValue(name: String) = name
    }

    implicit val stageTypeConfHint = new FieldCoproductHint[StageConfig]("stageType") {
      override def fieldValue(name: String) = name
    }

    val retConfigs = loadConfigOrThrow[IngestionConfig](config, rootKey)
    retConfigs
  }


  def getIngestionConfig(applicationConfigURI: URL, root: String): Either[Throwable, IngestionConfig] = {
    Either.catchNonFatal {
      val appConfig = ConfigFactory.parseURL(applicationConfigURI).resolve()
      get(appConfig, root)
    }
  }

  def findDatasetFromFileName(fileName: String, config: IngestionConfig): Either[String, DatasetConfig] = {
    Either.fromOption(
      config.datasets.find { dataSet =>
        val pattern = dataSet.filePattern.r
        pattern.findFirstIn(fileName).exists(_.nonEmpty)
      }
      , "Configuration for the fileName pattern not found")
  }

}