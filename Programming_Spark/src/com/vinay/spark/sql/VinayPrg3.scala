package com.vinay.spark.sql

object VinayPrg3 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.functions._

  val spSession = CommonUtils.spSession
  val spContext = CommonUtils.spContext

  // Create a DataFrame from a CSV file
  val autoDf = spSession.read.option("header", "true").csv(CommonUtils.dataDir + "Auto-Data.csv")

  Console.println("\n****** DataFrame Examples *******")

  Console.println("*********************************")
  Console.println("Auto Data-Set")
  Console.println("*********************************")
  autoDf.show() // ByDefault 20 records will be retrieved

  Console.println("*********************************")
  Console.println("Auto Schema")
  Console.println("*********************************")
  autoDf.printSchema()

  Console.println("*********************************")
  Console.println("DataFrame Query Example : Extracting 5 Records")
  Console.println("*********************************")
  autoDf.show(5)

  Console.println("*********************************")
  Console.println("DataFrame Query Example : Extracting Records using And Operation")
  Console.println("*********************************")
  autoDf.filter(autoDf("CYLINDERS") === "two" && autoDf("HP") === 101).show()

  Console.println("*********************************")
  Console.println("DataFrame Query Example : Extracting Records using Aggregate Functionswwww")
  Console.println("*********************************")
  autoDf.select(max(autoDf("RPM"))).show()
  
  Console.println("*********************************")
  Console.println("SparkSession SQL Example : Extracting Records using Temporary View")
  Console.println("*********************************")
  autoDf.createOrReplaceTempView("auto_dtls")
  spSession.sql("select * from auto_dtls").show()

  Console.println("*********************************")
  Console.println("SparkSession SQL Example : Extracting Records using And Operation")
  Console.println("*********************************")
  spSession.sql("select * from auto_dtls WHERE cylinders='two' and hp=101").show()

  Console.println("*********************************")
  Console.println("SparkSession SQL Example : Extracting Records using Aggregate Operation")
  Console.println("*********************************")
  spSession.sql("select upper(make), max(rpm) from auto_dtls group by make order by 2").show()
}

