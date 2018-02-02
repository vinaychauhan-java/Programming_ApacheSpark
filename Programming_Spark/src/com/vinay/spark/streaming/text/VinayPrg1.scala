package com.vinay.spark.streaming.text

object VinayPrg1 extends App {

  import com.vinay.spark.util._
  import org.apache.spark.streaming.StreamingContext
  import org.apache.spark.streaming.StreamingContext._
  import org.apache.spark.streaming.Seconds
  import org.apache.spark.rdd.RDD
  import java.io._

  val spSession = CommonUtils.spSession
  val spContext = CommonUtils.spContext

  // Create streaming Context with latency of 5 Seconds
  var ssc = new StreamingContext(spContext, Seconds(5))
  val dataLines = ssc.socketTextStream("localhost", 9000)
  // To See the Total Records processed and respective Time Frame
  println(dataLines.print())

  val logFileName = new File(CommonUtils.dataDir + "StreamingOutput.txt")
  logFileName.delete()
  val printWriter = new PrintWriter(logFileName)

  // Word Count within RDD
  val words = dataLines.flatMap(x => x.split(" "))
  val kvPairs = words.map(x => (x, 1))
  val wordCounts = kvPairs.reduceByKey((x, y) => x + y)
  println("Total Word Count : " + wordCounts.print())

  //Count lines
  def computeDataMetrics(rdd: RDD[String]): Long = {
    val linesCount = rdd.count()
    rdd.collect()
    println("Total Lines in RDD : " + linesCount + "\n")
    printWriter.write("Total Lines in RDD : " + linesCount + "\n")
    printWriter.flush()
    return linesCount
  }
  val lineCount = dataLines.foreachRDD(computeDataMetrics(_))

  //Compute window metrics
  def windowDataMetrics(rdd: RDD[String]) = {
    print("Window RDD Size :" + rdd.count() + "\n")
    printWriter.write("Window RDD Size :" + rdd.count() + "\n")
    printWriter.flush();
  }
  val windowedRDD = dataLines.window(Seconds(15), Seconds(5))
  windowedRDD.foreachRDD(windowDataMetrics(_))

  ssc.start()
  ssc.awaitTermination()
  printWriter.close()
}