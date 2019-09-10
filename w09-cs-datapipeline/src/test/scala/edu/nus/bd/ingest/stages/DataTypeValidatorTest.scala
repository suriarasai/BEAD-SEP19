package edu.nus.bd.ingest.stages

import edu.nus.bd.config.PipelineConfig.DataColumn
import edu.nus.bd.ingest.DataFrameOps
import edu.nus.bd.ingest.StageConstants.RowKey
import edu.nus.bd.ingest.base.{PipelineTestBase, SparkTestingBase}
import edu.nus.bd.ingest.models.ErrorModels.DataError
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class DataTypeValidatorTest extends FlatSpec with Matchers with SparkTestingBase with PipelineTestBase {

  "DataTypeValidatorTest" should "Validate and gather errors for all columns" in {

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val df = spark
      .sparkContext
      .parallelize(List(
        ("r1", "a", "1", "0"),
        ("r2", "x", "t", "5"),
        ("r3", "y", "2", "1.0"),
        ("r4", "z", "v", "u")
      ))
      .toDF(RowKey, "name", "age", "gpa")

    val dataCols = List(
      DataColumn("name", "string"),
      DataColumn("age", "long"),
      DataColumn("gpa", "double")
    )

    val stage = new DataTypeValidatorStage(dataCols)(spark)

    val (errors, returnDf) = stage(DataFrameOps.emptyDataErrors, df)

    returnDf.createOrReplaceTempView("DataTypeCastDf")

    returnDf.show(false)
    returnDf.printSchema()

    errors.collect().foreach(println)
    returnDf.schema.fields.map(_.dataType) shouldBe Array(StringType, StringType, StringType, StringType)

    errors.count() shouldBe 3
    errors.collect().toSet shouldBe Set(
      DataError("r2", "DataTypeValidatorStage", "age", "t", "Value could not be converted to the target datatype 'long'. Error: For input string: \"t\". Column config : DataColumn(age,long,,true,false).", "ErrorSeverity", ""),
      DataError("r4", "DataTypeValidatorStage", "age", "v", "Value could not be converted to the target datatype 'long'. Error: For input string: \"v\". Column config : DataColumn(age,long,,true,false).", "ErrorSeverity", ""),
      DataError("r4", "DataTypeValidatorStage", "gpa", "u", "Value could not be converted to the target datatype 'double'. Error: For input string: \"u\". Column config : DataColumn(gpa,double,,true,false).", "ErrorSeverity", "")
    )


  }
}
