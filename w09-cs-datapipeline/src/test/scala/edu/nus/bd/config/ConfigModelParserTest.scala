package edu.nus.bd.config

import edu.nus.bd.ingest.base.PipelineTestBase
import org.scalatest.{FlatSpec, Matchers}

class ConfigModelParserTest extends FlatSpec with Matchers with PipelineTestBase {

  "ConfigModelParserTest" should "parse all configs from the config correctly" in {

    val ingestionConfig = getIngestionConfig("/datasets_test.conf")

    val dataSets = ingestionConfig.datasets

    val firstDataSet = dataSets.head
    firstDataSet.dataShape shouldBe "Fact"
  }
}
