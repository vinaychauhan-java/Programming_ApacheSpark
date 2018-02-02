package com.vinay.spark

import java.io.File

object VinayPrg3 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf

  //Used to avoid :- java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
  // System.setProperty("hadoop.home.dir", "C:/Installation/HadoopFiles/winutils.exe");

  //Workaround to avoid :- java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
  System.getProperties().put("hadoop.home.dir", new File(".").getAbsolutePath());
  new File("./bin").mkdirs();
  new File("./bin/winutils.exe").createNewFile();

  //A name for the spark instance. Can be any string
  val appName = "Programming_Spark"

  //Pointer /URL to the Spark instance - embedded
  //val sparkMasterURL = "spark://169.254.116.226:7077"
  //val sparkMasterURL = "spark://localhost:7077"
  val sparkMasterURL = "local[2]"

  //Create a configuration object
  val conf = new SparkConf().setAppName(appName).setMaster(sparkMasterURL).set("spark.executor.memory", "1g")

  //Start a Spark Session
  //val spContext = SparkContext.getOrCreate(conf)
  val spContext = new SparkContext(conf)

  //Check http://localhost:4040

  //Load a data file into an RDD
  val tweetsRDD = spContext.textFile(CommonUtils.dataDir + "MovieTweets.csv")

  //print first five lines
  for (tweet <- tweetsRDD.take(5)) println(tweet)

  //Print number of lines in file  and this is called lazy evaluation
  println("Total tweets in file :" + tweetsRDD.count())

  //Convert tweets to upper case
  val tweetsUpper = tweetsRDD.map(s => s.toUpperCase())

  //Print the converted items.
  for (tweet <- tweetsUpper.take(5)) println(tweet)

}

// Set the existing SparkContext's Master, AppName and other params
// sc.getConf.setMaster("local[2]").setAppName("NetworkWordCount").set("spark.ui.port", "44040" )
// Use 'sc' to create a Streaming context with 2 second batch interval
// val ssc = new StreamingContext(sc, Seconds(2) )