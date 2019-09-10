package edu.nus.bd.ingest.stages

import edu.nus.bd.config.PipelineConfig.DataColumn
import edu.nus.bd.ingest.models.ErrorModels.DataError
import edu.nus.bd.ingest.stages.base.DataStage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import edu.nus.bd.ingest.StageConstants._
import edu.nus.bd.ingest.UDFs.generateUUID


class AddRowKeyStage(dataCols: List[DataColumn])
                    (implicit spark: SparkSession, encoder: Encoder[DataError])
  extends DataStage[DataFrame] {

  override val stage: String = getClass.getSimpleName

  def apply(errors: Dataset[DataError], data: DataFrame): (Dataset[DataError], DataFrame) = {
    val colOrder = RowKey +: dataCols.map(_.name)
    val withRowKeyDf = data.withColumn(RowKey, generateUUID()).cache()
    val returnDf = withRowKeyDf.select(colOrder.map(col): _*)
    (spark.emptyDataset[DataError], returnDf)
  }
}
