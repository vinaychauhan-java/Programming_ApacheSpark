package com.vinay.spark

object VinayPrg7 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf

  val spContext = CommonUtils.spContext

  /**
   * .........................................
   * Loading Data From a File & create a Key/Value RDD of AutoBrand and Horsepower
   * .........................................
   */
  val autoRDD = spContext.textFile(CommonUtils.dataDir + "Auto-Data.csv")
  autoRDD.cache()
  val headerData = autoRDD.first
  var autoFilterData = autoRDD.filter(s => !s.equals(headerData))
  val autoKVRDD = autoFilterData.map(x => (x.split(",")(0), x.split(",")(7)))
  Console.println("Total Key/Value Auto Count : " + autoKVRDD.count());
  Console.println("Total Distince Keys Count  : " + autoKVRDD.distinct.count());
  Console.println("Key/Value Data-Set")
  for (x <- autoKVRDD.take(5)) { println(x) }
}