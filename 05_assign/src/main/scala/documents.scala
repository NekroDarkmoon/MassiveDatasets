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

object Document extends App {

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
  val langFile = Source.fromFile("data/languages.csv").getLines().drop(1)

  // Create languages index
  val languages: Map[String, Int] =
    langFile.foldLeft(Map[String, Int]()) { (acc, lang) =>
      val str = lang.trim.split(",").reverse.toList

      acc + (str.head -> str.last.toInt)
    }

  // Create output lines
  val outputLines: List[String] = lines
    .map(line => {
      val bits: List[String] = line.trim.split(",").toList
      val uid: String = bits.head
      val langs: List[String] = bits.tail

      val langDocs = langs
        .map(l => languages.getOrElse(l, -1))
        .sorted
        .map(l => s"$l:1")

      s"$uid ${langDocs.mkString(" ")}"
    })
    .collect()
    .toList

  outputLines.take(33).foreach(println)

  // Send to output file
  println(s"Writing to file")

  val ofile = new File(outputFilename)
  val buffer = new BufferedWriter(new FileWriter(ofile))

  outputLines.foreach(line => buffer.write(s"$line\n"))

  buffer.close()

  sc.stop()

}
