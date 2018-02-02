package com.vinay.spark

object VinayPrg5 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf

  val spContext = CommonUtils.spContext
  val fruits1 = spContext.parallelize(Array("Mango", "Orange", "Apple", "Banana"))
  val frutis2 = spContext.parallelize(Array("Grapes", "PineApple", "Apple"))

  Console.println("\n\n")
  Console.println("*********************************")
  Console.println("Union Operation")
  Console.println("*********************************")
  for (x <- fruits1.union(frutis2).distinct().collect()) { println(x) }

  Console.println("\n\n")
  Console.println("*********************************")
  Console.println("Intersection Operation")
  Console.println("*********************************")
  for (x <- fruits1.intersection(frutis2).collect()) { println(x) }

}