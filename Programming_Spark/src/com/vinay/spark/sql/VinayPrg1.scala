package com.vinay.spark.sql

object VinayPrg1 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.functions._

  val spSession = CommonUtils.spSession
  val spContext = CommonUtils.spContext

  // Create a DataFrame from a Data JSON file
  val empDf = spSession.read.json(CommonUtils.dataDir + "EmployeeData.json")
  
  Console.println("\n****** DataFrame Examples *******")

  Console.println("*********************************")
  Console.println("Employee Data-Set")
  Console.println("*********************************")
  empDf.show()

  Console.println("*********************************")
  Console.println("Employee Schema")
  Console.println("*********************************")
  empDf.printSchema()

  Console.println("*********************************")
  Console.println("DataFrame Query Example : Select")
  Console.println("*********************************")
  empDf.select("name", "salary").show()

  Console.println("*********************************")
  Console.println("DataFrame Query Example : Filter")
  Console.println("*********************************")
  empDf.filter(empDf("age") <= "32").show()
  empDf.filter(empDf("age") >= "32").show()
  empDf.select("name", "salary").filter(empDf("age") === "32").show()

  Console.println("*********************************")
  Console.println("DataFrame Query Example : GroupBy")
  Console.println("*********************************")
  empDf.groupBy("gender").count().show()

  Console.println("*********************************")
  Console.println("DataFrame Query Example : Aggregate")
  Console.println("*********************************")
  empDf.groupBy("deptid").agg(avg(empDf("salary")), max(empDf("age"))).show()
}