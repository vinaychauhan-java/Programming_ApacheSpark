package com.vinay.spark

object VinayPrg6 extends App {

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
  Console.println("\n\n")
  Console.println("*********************************")
  Console.println("Action : Executing Reduce Functions")
  Console.println("*********************************")
  Console.println(myRDD.reduce((x, y) => x + y))

  /**
   * .........................................
   * Loading Data From a File
   * .........................................
   */
  val autoRDD = spContext.textFile(CommonUtils.dataDir + "Auto-Data.csv")
  autoRDD.cache()
  Console.println("Total Auto Count  : " + autoRDD.count());
  Console.println("Data Valdation    : " + autoRDD.reduce((x, y) => if (x.length() < y.length) x else y))

  //Use a function to extract MPG from Data-Set
  def getMPG(autoDataString: String): String = {
    if (isAllDigits(autoDataString)) {
      return autoDataString
    } else {
      val attList = autoDataString.split(",")
      if (isAllDigits(attList(9))) {
        return attList(9)
      } else {
        return "0"
      }
    }
  }

  def isAllDigits(x: String) = x.matches("^\\d*$")

  //Find average MPG-City for all cars
  val totMPG = autoRDD.reduce((x, y) => (getMPG(x).toInt + getMPG(y).toInt).toString)
  Console.println("Average MPG-City for all cars : " + totMPG.toInt / (autoRDD.count()))

}