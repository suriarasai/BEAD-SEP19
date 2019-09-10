package edu.nus.bd.ingest.stages.base

import edu.nus.bd.ingest.models.ErrorModels.DataError
import org.apache.spark.sql.Dataset

trait DataStage[T <: Dataset[_]] extends Serializable {
  def apply(errors: Dataset[DataError], data: T): (Dataset[DataError], T)
  def stage: String
}
