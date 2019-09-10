package edu.nus.bd.ingest.stages

import edu.nus.bd.config.PipelineConfig.DataColumn
import edu.nus.bd.ingest.DataFrameOps
import edu.nus.bd.ingest.models.ErrorModels.DataError
import edu.nus.bd.ingest.stages.base.DataStage
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import edu.nus.bd.ingest.StageConstants._

class DataTypeCastStage(dataCols: List[DataColumn])(implicit spark: SparkSession, encoder: Encoder[DataError]) extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(errors: Dataset[DataError], data: DataFrame): (Dataset[DataError], DataFrame) = {

    val origColOrder = RowKey :: dataCols.map(_.name)

    val unorderedDf =
      dataCols.foldLeft(data) { case (df, configCol) => CatalystSqlParser.parseDataType(configCol.dType) match {
        case StringType => df
        case IntegerType => df.withColumn(configCol.name, df(configCol.name).cast(IntegerType))
        case LongType => df.withColumn(configCol.name, df(configCol.name).cast(LongType))
        case FloatType => df.withColumn(configCol.name, df(configCol.name).cast(FloatType))
        case DoubleType => df.withColumn(configCol.name, df(configCol.name).cast(DoubleType))
        case BooleanType => df.withColumn(configCol.name, df(configCol.name).cast(BooleanType))
        case DateType => {
          println(configCol.name)
          df.withColumn(configCol.name, to_date(unix_timestamp(df(configCol.name), configCol.format).cast(TimestampType)))
        }
        case x@_ => println(s"Casting of type $x not implemented"); df
      }
      }

    val returnDf = unorderedDf.select(origColOrder.map(col): _*)
    (errors.union(DataFrameOps.emptyDataErrors), returnDf)
  }
}
