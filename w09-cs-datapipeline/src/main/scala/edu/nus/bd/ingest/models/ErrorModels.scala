package edu.nus.bd.ingest.models

import org.apache.spark.sql.Row

object ErrorModels {

  case class DataError(
                        rowKey: String,
                        stage: String,
                        fieldName: String,
                        fieldValue: String,
                        error: String,
                        severity: String,
                        addlInfo: String = ""
                      )

  object DataError{
    def apply(row: Row): DataError = new DataError(
      row.getAs[String]("rowKey"),
      row.getAs[String]("stage"),
      row.getAs[String]("fieldName"),
      row.getAs[String]("fieldValue"),
      row.getAs[String]("error"),
      row.getAs[String]("severity"),
      row.getAs[String]("addlInfo")
    )
  }
}
