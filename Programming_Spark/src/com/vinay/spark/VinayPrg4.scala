package com.vinay.spark

object VinayPrg4 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf

  val spContext = CommonUtils.spContext

  /**
   * .........................................
   * Loading Data From a Collection
   * .........................................
   */
  val myRDD = spContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
  myRDD.cache()
  Console.println("Total Count : " + myRDD.count());

  /**
   * .........................................
   * Loading Data From a File
   * .........................................
   */
  val autoRDD = spContext.textFile(CommonUtils.dataDir + "Auto-Data.csv")
  autoRDD.cache()
  //Loads only Now....
  Console.println("Total Auto Count            : " + autoRDD.count());
  Console.println("First Auto Information      : " + autoRDD.first());
  Console.println("First Five Auto Information : " + autoRDD.take(5));

  Console.println("\n\n")
  Console.println("*********************************")
  Console.println("Extracting Auto Data-Set")
  Console.println("*********************************")
  for (x <- autoRDD.take(5)) { println(x) }

  /**
   * .........................................
   * Transformations
   * .........................................
   */
  var transformedData = autoRDD.map(x => x.replace(",", "\t"))
  Console.println("\n")
  Console.println("*********************************")
  Console.println("Extracting Trasnformed Auto Data-Set")
  Console.println("*********************************")
  for (x <- transformedData.take(5)) { println(x) }

  // Removing First header line
  val headerData = autoRDD.first
  var autoFilterData = autoRDD.filter(s => !s.equals(headerData))
  Console.println("\n")
  Console.println("Total Auto Count without Header :" + autoFilterData.count());

  // Filter only for Toyota cars and create a new RDD
  autoFilterData = autoRDD.filter(s => s.contains("toyota"))
  Console.println("\n")
  Console.println("Total Auto Count with Toyota    : " + autoFilterData.count());

  //Cleanse and Transform an RDD with a function
  def cleanseAutoRDD(autoString: String): String = {
    val attributeList = autoString.split(",")
    if (attributeList(3).matches("two")) {
      attributeList(3) = "2"
    } else {
      attributeList(3) = "4"
    }
    attributeList(5) = attributeList(5).toUpperCase() //convert Drive to UpperCase
    attributeList.mkString(",") // Rebuilding the Data-Set
  }

  val cleanedAutoRDD = autoRDD.map(cleanseAutoRDD)
  //val cleansedAutoList = cleanedAutoRDD.collect()
  val cleansedAutoList = cleanedAutoRDD.take(5)
  Console.println("\n")
  Console.println("*********************************")
  Console.println("Extracting Records after Cleansing & Transforamtion");
  Console.println("*********************************")
  for (x <- cleansedAutoList) { println(x) }
}