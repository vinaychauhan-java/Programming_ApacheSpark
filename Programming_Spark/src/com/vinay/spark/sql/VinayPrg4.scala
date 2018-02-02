package com.vinay.spark.sql

object VinayPrg4 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Row;
  import org.apache.spark.sql.types._

  val spSession = CommonUtils.spSession
  val spContext = CommonUtils.spContext

  val schema = StructType(StructField("id", IntegerType, false) :: StructField("BoolName", StringType, false) :: Nil)

  val BookList = Array((1001, "Java"), (1002, "Spark"), (1003, "Hadoop"))

  val bookRDD = spContext.parallelize(BookList)

  Console.println("\n*****Struct Type examples *****")

  Console.println("*********************************")
  Console.println("BookRDD Data-Set :: Count ")
  Console.println("*********************************")
  Console.println("BookRDD Count " + bookRDD.count())
  //bookRDD.collect()

  // In this method, input is a tuple and output is a Row
  def transformToRow(input: (Int, String)): Row = {
    //We can filter out columns not wanted at this stage
    val values = Row(input._1, input._2)
    return values
  }
  val bookUpdatedRDD = bookRDD.map(transformToRow)
  val prodDf = spSession.createDataFrame(bookUpdatedRDD, schema)

  Console.println("*********************************")
  Console.println("BookRDD Data-Set :: Records Retrieved")
  Console.println("*********************************")
  Console.println("BookRDD Count " + bookRDD.count())
  prodDf.show()

}