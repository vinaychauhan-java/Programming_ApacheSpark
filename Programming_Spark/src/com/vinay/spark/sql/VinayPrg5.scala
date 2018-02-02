package com.vinay.spark.sql

object VinayPrg5 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Row;
  import org.apache.spark.sql.types._

  val spSession = CommonUtils.spSession
  val spContext = CommonUtils.spContext

  /**
   * Case Study :-
   * 1. Load that "iris.csv" file into a Spark SQL Data Frame called "irisDF".
   * 2. In the irisDF, filter for all rows whose PetalWidth is greater than 0.4 and count them.
   */

  Console.println("*********************************")
  Console.println("Iris Data-Set")
  Console.println("*********************************")

  //  Create a DataFrame from a CSV file
  //  val irisDf = spSession.read.option("header", "true").csv(CommonUtils.dataDir + "Iris.csv")
  //  irisDf.show()

  val irisData = spContext.textFile(CommonUtils.dataDir + "Iris.csv")
  val irisFilteredData = irisData.filter(x => !x.contains("Sepal"))
  irisFilteredData.cache()
  for (x <- irisFilteredData.take(5)) { println(x) }

  import org.apache.spark.ml.linalg.{ Vector, Vectors }
  import org.apache.spark.ml.feature.LabeledPoint
  import org.apache.spark.sql.Row;
  import org.apache.spark.sql.types._

  // Schema for Data Frame
  val schema =
    StructType(
      StructField("SPECIES", StringType, false) ::
        StructField("SEPAL_LENGTH", DoubleType, false) ::
        StructField("SEPAL_WIDTH", DoubleType, false) ::
        StructField("PETAL_LENGTH", DoubleType, false) ::
        StructField("PETAL_WIDTH", DoubleType, false) :: Nil)

  def transformDataToRow(inputString: String): Row = {
    val attList = inputString.split(",")
    val rowData = Row(
      attList(4),
      attList(0).toDouble,
      attList(1).toDouble,
      attList(2).toDouble,
      attList(3).toDouble)
    return rowData
  }

  Console.println("*********************************")
  Console.println("Iris Transformed Data-Set")
  Console.println("*********************************")
  //Change to a Vector
  val irisVectors = irisFilteredData.map(transformDataToRow)
  for (x <- irisVectors.take(5)) { println(x) }

  Console.println("*********************************")
  Console.println("Iris DataFrame")
  Console.println("*********************************")
  val irisDF = spSession.createDataFrame(irisVectors, schema)
  irisDF.printSchema()
  irisDF.show(5)

  println("*********************************")
  println("Iris DataFrame Operations")
  println("*********************************")
  irisDF.filter(irisDF("PETAL_WIDTH") > 0.4).show()
  println("Petal Width > 0.44 = " + irisDF.filter(irisDF("PETAL_WIDTH") > 0.4).count())

  println("*********************************")
  println("Iris Temp Table Operations")
  println("*********************************")
  irisDF.createOrReplaceTempView("iris_dtl")
  spSession.sql("select species, round(avg(petal_width)) from iris_dtl group by SPECIES").show()

}