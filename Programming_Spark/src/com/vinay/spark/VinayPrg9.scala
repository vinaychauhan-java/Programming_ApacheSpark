package com.vinay.spark

object VinayPrg9 extends App {

  import com.vinay.spark.util._

  val spContext = CommonUtils.spContext

  /**
   * .........................................
   * Advanced Spark : Partitions
   * .........................................
   */

  //Specify no. of partitions.
  val myCollection = spContext.parallelize(Array(3, 5, 4, 7, 4), 4)
  Console.println("Total No. of Partitions : " + myCollection.getNumPartitions)

}