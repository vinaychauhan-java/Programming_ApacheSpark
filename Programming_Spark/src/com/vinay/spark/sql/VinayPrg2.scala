package com.vinay.spark.sql

object VinayPrg2 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.functions._

  val spSession = CommonUtils.spSession
  val spContext = CommonUtils.spContext

  // Create a DataFrame from a Data JSON file
  val empDf = spSession.read.json(CommonUtils.dataDir + "EmployeeData.json")

  // Create a DataFrame from an array of JSON object
  val deptList = Array("{'name': 'Sales', 'id': '100'}", "{'name':'Engineering','id':'200' }")
  // Convert list to RDD
  val deptRDD = spContext.parallelize(deptList)

  //Load RDD into a DataFrame
  val deptDf = spSession.read.json(deptRDD)

  Console.println("\n****** DataFrame Examples *******")

  Console.println("*********************************")
  Console.println("Department Data-Set")
  Console.println("*********************************")
  deptDf.show()

  Console.println("*********************************")
  Console.println("Department Schema")
  Console.println("*********************************")
  deptDf.printSchema()
  //deptDf.explain()

  Console.println("*********************************")
  Console.println("DataFrame Query Example : Joins")
  Console.println("*********************************")
  // empDf.join(deptDf, empDf("deptid") === deptDf("id")).orderBy("age").show()
  // -- Any operation on DataFrame will create another DataFrame (Similar like RDD!!)
  // -- DataFrames are Immutable 
  val empDFSumarry = empDf.join(deptDf, empDf("deptid") === deptDf("id")).orderBy("age")
  empDFSumarry.show()

  Console.println("*********************************")
  Console.println("DataFrame Query Example : Cascade Operations-1")
  Console.println("*********************************")
  empDf.filter(empDf("age") > 30).join(deptDf, empDf("deptid") === deptDf("id")).groupBy("deptid").agg(avg("salary"), max("age")).show()

  Console.println("*********************************")
  Console.println("DataFrame Query Example : Cascade Operations-2")
  Console.println("*********************************")
  empDf.join(deptDf, empDf("deptid") === deptDf("id")).filter(empDf("age") > 30 && deptDf("id") === 100).groupBy("deptid").agg(avg("salary"), max("age")).show()

}