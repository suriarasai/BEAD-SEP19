package edu.nus.bd.ingest.stages

import edu.nus.bd.config.PipelineConfig.DataColumn
import edu.nus.bd.ingest.DataFrameOps
import edu.nus.bd.ingest.UDFs.replaceCharUdf
import edu.nus.bd.ingest.models.ErrorModels.DataError
import edu.nus.bd.ingest.stages.base.DataStage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

class ReplaceCharDataStage(replaceMap: Map[String, String],
                           dataCols:List[DataColumn],
                           applicableCols: Seq[String])
                          (implicit spark: SparkSession, encoder: Encoder[DataError])
  extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(errors: Dataset[DataError], data: DataFrame): (Dataset[DataError], DataFrame) = {

    val origColOrder = dataCols.map(_.name)
    val unorderedDf = applicableCols.foldLeft(data) { case (df, col) =>
      df.withColumn(col, replaceCharUdf(replaceMap)(df(col)))
    }
    val returnDf = unorderedDf.select(origColOrder.map(col): _*)
    (DataFrameOps.emptyDataErrors, returnDf)
  }
}
