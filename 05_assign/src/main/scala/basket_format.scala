package stackoverflow

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter

object BasketFormat extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("MinHash")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR") // avoid all those messages going on

  val filename = args(0)
  val outputFilename = args(1)

  // remove line below and replace with your code
  println("Reading from file ", filename)

  // Import file
  val lines = sc.textFile(filename)

  // Drop header
  val headerless = lines.mapPartitionsWithIndex((idx, line) =>
    if (idx == 0) line.drop(1) else line
  )

  // Morph input into output
  val outputLines = headerless.map(line => {
    val bits = line.split(",", 2).toList
    val index = bits.head.trim.replace("\"", "")
    val langs = bits.last
      .replace("{", "")
      .replace("}", "")
      .replace("\"", "")
      .split(",")
      .toList

    s"$index,${langs.mkString(",")}"
  })

  // Send to output file
  println(s"Writing to file")

  val ofile = new File(outputFilename)
  val buffer = new BufferedWriter(new FileWriter(ofile))

  outputLines.collect().foreach(line => buffer.write(s"$line\n"))

  buffer.close()

  sc.stop()

}
