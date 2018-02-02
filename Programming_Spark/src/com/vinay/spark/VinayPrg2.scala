package com.vinay.spark

import org.apache.spark.SparkContext, org.apache.spark.SparkConf
import java.io.File

object VinayPrg2 {
  def main(args: Array[String]) {
    //Used to avoid :- java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "C:/Installation/HadoopFiles/winutils.exe");

    val conf = new SparkConf().setAppName("Programming_Spark").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val i = List(1, 2, 3, 4, 5)
    val dataRDD = sc.parallelize(i)
    dataRDD.saveAsTextFile("D:/DataExtracted1")
  }
}