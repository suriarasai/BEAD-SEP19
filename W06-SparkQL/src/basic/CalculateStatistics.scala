package basic

import org.apache.spark.sql.types.DataTypes
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions.col

object CalculateStatistics {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    case class Call(age: Int, job: String, marital: String, edu: String, credit_default: String, housing: String, loan: String, contact: String, month: String, day: String, dur: Double, campaign: Double, pdays: Double, prev: Double, pout: String, emp_var_rate: Double, cons_price_idx: Double, cons_conf_idx: Double, euribor3m: Double, nr_employed: Double, deposit: String)

    // Create the Spark Session and the spark context
    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

    import sqlcontext.implicits._

    val age = StructField("age", DataTypes.IntegerType)
    val job = StructField("job", DataTypes.StringType)
    val marital = StructField("marital", DataTypes.StringType)
    val edu = StructField("edu", DataTypes.StringType)
    val credit_default = StructField("credit_default", DataTypes.StringType)
    val housing = StructField("housing", DataTypes.StringType)
    val loan = StructField("loan", DataTypes.StringType)
    val contact = StructField("contact", DataTypes.StringType)
    val month = StructField("month", DataTypes.StringType)
    val day = StructField("day", DataTypes.StringType)
    val dur = StructField("dur", DataTypes.DoubleType)
    val campaign = StructField("campaign", DataTypes.DoubleType)
    val pdays = StructField("pdays", DataTypes.DoubleType)
    val prev = StructField("prev", DataTypes.DoubleType)
    val pout = StructField("pout", DataTypes.StringType)
    val emp_var_rate = StructField("emp_var_rate", DataTypes.DoubleType)
    val cons_price_idx = StructField("cons_price_idx", DataTypes.DoubleType)
    val cons_conf_idx = StructField("cons_conf_idx", DataTypes.DoubleType)
    val euribor3m = StructField("euribor3m", DataTypes.DoubleType)
    val nr_employed = StructField("nr_employed", DataTypes.DoubleType)
    val deposit = StructField("deposit", DataTypes.StringType)

    val fields = Array(age, job, marital, edu, credit_default, housing, loan, contact, month, day, dur, campaign, pdays, prev, pout, emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed, deposit)
    val schema = StructType(fields)

    val df = spark.read.schema(schema).option("sep", ";").option("header", true).csv("/home/cloudera/git/BEAD-SEP19/W06-SparkQL/data/bank-additional-full.csv")
    //Code for Computing Basic Statistics section
    case class CallStats(age: Int, dur: Double, campaign: Double, prev: Double, deposit: String)
    val subsetDF = df.select(col("age"), col("dur"), col("campaign"), col("prev"), col("deposit"))
    subsetDF.show(5)
    subsetDF.describe().show()
    val cov = subsetDF.stat.cov("age", "dur")
    println("age to call duration : Covariance = %.4f".format(cov))
    val corr = subsetDF.stat.corr("age", "dur")
    println("age to call duration : Correlation = %.4f".format(corr))
    df.stat.crosstab("age", "marital").orderBy("age_marital").show(10)
    
  }
}