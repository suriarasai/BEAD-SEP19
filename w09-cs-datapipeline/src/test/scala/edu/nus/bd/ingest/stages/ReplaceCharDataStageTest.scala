package edu.nus.bd.ingest.stages

import edu.nus.bd.config.PipelineConfig.DataColumn
import edu.nus.bd.ingest.DataFrameOps
import edu.nus.bd.ingest.base.SparkTestingBase
import org.scalatest.{FlatSpec, Matchers}

case class RawStudentRep(name: String, age: String, gpa: String)

class ReplaceCharDataStageTest extends FlatSpec with Matchers with SparkTestingBase {

  "ReplaceCharTransformer" should "replace the find values with replace values correctly for all columns" in {

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val df = spark
      .sparkContext
      .parallelize(List(
        RawStudentRep("?", "1", "0"),
        RawStudentRep("x", "?", "0"),
        RawStudentRep("y", "2", "?"),
        RawStudentRep("?", "?", "?")
      ))
      .toDF

    val dataCols = List(
      DataColumn("name", "string"),
      DataColumn("age", "long"),
      DataColumn("gpa", "double")
    )

    val replaceMap = Map ("?" -> "REPLACED")

    val stage = new ReplaceCharDataStage(replaceMap, dataCols, Seq ("name", "age", "gpa"))

    val (errors, returnDf) = stage(DataFrameOps.emptyDataErrors, df)

    returnDf.createOrReplaceTempView("ReplaceCharDf")

    returnDf.show(false)

    spark.sql("select count(*) from ReplaceCharDf where name = 'REPLACED'").head().getLong(0) shouldBe 2
    spark.sql("select count(*) from ReplaceCharDf where age = 'REPLACED'").head().getLong(0) shouldBe 2
    spark.sql("select count(*) from ReplaceCharDf where gpa = 'REPLACED'").head().getLong(0) shouldBe 2

  }
}
