package edu.nus.bd.ingest.stages

import edu.nus.bd.config.PipelineConfig.DataColumn
import edu.nus.bd.ingest.StageConstants._
import edu.nus.bd.ingest.UDFs.validateRowUDF
import edu.nus.bd.ingest.models.ErrorModels.DataError
import edu.nus.bd.ingest.stages.base.DataStage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class DataTypeValidatorStage(dataCols: List[DataColumn])(implicit val spark: SparkSession) extends DataStage[DataFrame] {

  override val stage = getClass.getSimpleName

  def apply(errors: Dataset[DataError], data: DataFrame): (Dataset[DataError], DataFrame) = {

    val withErrorsDF = data.withColumn(RowLevelErrorListCol, validateRowUDF(dataCols, stage)(struct(data.columns.map(data(_)): _*)))

    import spark.implicits._

    val errorRecords =
      withErrorsDF
        .select(RowLevelErrorListCol)
        .select(explode(col(RowLevelErrorListCol)))
        .select("col.*")
        .map(row => DataError(row))

    (errorRecords.union(errors), withErrorsDF.drop(RowLevelErrorListCol).join(errorRecords, Seq(RowKey), "leftanti"))
  }
}