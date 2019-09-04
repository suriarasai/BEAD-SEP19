package sqlsamples

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SparkDataFrameExample extends App {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  	// Create the Spark Session and the spark context				
	val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.master("local[2]")
			.getOrCreate()
	val sc = spark.sparkContext
	import spark.implicits._
	
	val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
	
	//Read the JSON Document
  
  val dfs = sqlcontext.read.json("/home/cloudera/git/BEAD-SEP19/W06-SparkQL/data/employees.json")
  
  // Show the data
  dfs.show()

  // Use Print Schema Method
  dfs.printSchema()
  
  // Use Selection
  dfs.select("name").show()
  
  // Use Filters
  dfs.filter(dfs("age") > 23).show()
  
  // Use Group By
  dfs.groupBy("age").count().show()
  
 
  
  
}