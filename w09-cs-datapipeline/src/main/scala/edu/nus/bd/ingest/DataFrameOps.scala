package edu.nus.bd.ingest

import cats.kernel.Semigroup
import edu.nus.bd.ingest.models.ErrorModels.DataError
import org.apache.spark.sql._

object DataFrameOps {

  implicit def dataFrameSemigroup[A: Encoder]: Semigroup[Dataset[A]] = new Semigroup[Dataset[A]] {
    override def combine(x: Dataset[A], y: Dataset[A]): Dataset[A] = x.union(y)
  }
  implicit val errorEncoder = Encoders.product[DataError]

  def emptyDataErrors(implicit spark:SparkSession) = spark.emptyDataset[DataError]
}
