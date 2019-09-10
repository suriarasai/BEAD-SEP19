package edu.nus.bd.ingest.stages

import edu.nus.bd.config.PipelineConfig.DataColumn
import edu.nus.bd.ingest.DataFrameOps
import edu.nus.bd.ingest.DataFrameOps.errorEncoder
import edu.nus.bd.ingest.StageConstants.RowKey
import edu.nus.bd.ingest.base.{PipelineTestBase, SparkTestingBase}
import org.scalatest.{FlatSpec, Matchers}

case class RawStudentAdd(name: String, age: String, gpa: String)

class AddRowKeyStageTest extends FlatSpec with Matchers with SparkTestingBase with PipelineTestBase {

  "AddRowKeyStage" should "have added the RowKeys for all rows" in {

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val df = spark
      .sparkContext
      .parallelize(List(
        RawStudentAdd("a", "1", "0"),
        RawStudentAdd("b", "?", "0"),
        RawStudentAdd("c", "2", "?"),
        RawStudentAdd("d", "?", "?")
      ))
      .toDF

    val dataCols = List(
      DataColumn("name", "string"),
      DataColumn("age", "long"),
      DataColumn("gpa", "double")
    )

    val stage = new AddRowKeyStage(dataCols)
    val (errors, returnDf) = stage(DataFrameOps.emptyDataErrors, df)
    returnDf.show(false)
    returnDf.select(RowKey).dropDuplicates().count() shouldBe 4
    errors.count() shouldBe 0
  }
}
