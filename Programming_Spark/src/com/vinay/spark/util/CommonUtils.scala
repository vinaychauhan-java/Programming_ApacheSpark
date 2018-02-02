package com.vinay.spark.util

object CommonUtils {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  
  import org.apache.log4j.Logger
  import org.apache.log4j.Level;

  Logger.getLogger("org").setLevel(Level.ERROR);
  Logger.getLogger("akka").setLevel(Level.ERROR);

  // Directory where the data files for Programming Spark exists
  val dataDir = "D:/_VinayWork/MyTestWorkSpace-11/Programming_Spark/dataFiles/"

  // Name for the Spark instance
  val appName = "Programming_Spark"

  // Pointer URL to the embedded Spark instance
  val sparkMasterURL = "local[2]"

  // Temp dir required for Spark SQL
  val tempDir = "file:///c:/tmp/spark-warehouse"

  var spSession: SparkSession = null
  var spContext: SparkContext = null

  // Initialization to be used when object is created
  {
    // Need to set hadoop.home.dir to avoid errors during startup
    System.setProperty("hadoop.home.dir", "C:/Installation/HadoopFiles/winutils");

    // Create Spark configuration object
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMasterURL)
      .set("spark.executor.memory", "1g")
      .set("spark.sql.shuffle.partitions", "2")

    // Get or create a spark context. Creates a new instance if not exists
    spContext = SparkContext.getOrCreate(conf)

    // Create a spark SQL session
    spSession = SparkSession
      .builder()
      .appName(appName)
      .master(sparkMasterURL)
      .config("spark.sql.warehouse.dir", tempDir)
      .getOrCreate()
  }

}