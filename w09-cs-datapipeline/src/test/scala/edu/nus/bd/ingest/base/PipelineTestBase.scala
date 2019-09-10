package edu.nus.bd.ingest.base

import edu.nus.bd.config.PipelineConfig
import edu.nus.bd.config.PipelineConfig.{DatasetConfig, IngestionConfig}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait PipelineTestBase extends BeforeAndAfterAll {
  self: Suite =>

  def getIngestionConfig(configPath: String): IngestionConfig = {
    val configUri = getClass.getResource(configPath)
    PipelineConfig.getIngestionConfig(configUri, "ingestionConfig").right.get
  }

  def getDataSetConfig(configPath: String, fileName: String): DatasetConfig = {
    PipelineConfig.findDatasetFromFileName(fileName, getIngestionConfig(configPath)).right.get
  }
}
