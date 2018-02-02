package com.vinay.spark

object VinayPrg8 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf

  val spContext = CommonUtils.spContext

  /**
   * .........................................
   * Advanced Spark : Accumulators & Broadcast Variables
   * .........................................
   */
  val autoRDD = spContext.textFile(CommonUtils.dataDir + "Auto-Data.csv")
  autoRDD.cache()

  // Initialize Accumulators
  val sedanCount = spContext.longAccumulator
  val hatchBackCount = spContext.longAccumulator

  // Set BroadCast Variables
  val sedanBC = spContext.broadcast("sedan")
  val hatchBackBC = spContext.broadcast("hatchback")

  def splitLines(dataLine: String): Array[String] = {
    if (dataLine.contains(sedanBC.value)) {
      sedanCount.add(1)
    }
    if (dataLine.contains(hatchBackBC.value)) {
      hatchBackCount.add(1)
    }
    dataLine.split(",")
  }

  // Doing Mapping for the Data-Set
  val splitData = autoRDD.map(splitLines)

  //Make it execute for the Map (Lazy Execution)
  Console.println("Total Split Count : " + splitData.count());

  println(sedanCount.value + " ::  " + hatchBackCount.value)

}