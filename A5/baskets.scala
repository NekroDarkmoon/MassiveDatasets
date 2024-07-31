package stackoverflow

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.log4j.{Level, Logger}
import java.io.File

object Baskets extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("MinHash")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR") // avoid all those messages going on

  val filename = args(0)

  // remove line below and replace with your code
  println("Reading from file ", filename)

  // Import file
  val lines = sc.textFile(filename)

  println(s"Running FPGrowth Algorithm.")
  // Prep data for algorithm
  val transactions: RDD[Array[String]] =
    lines.map(line => line.trim.split(",").tail)

  val fpg = new FPGrowth().setMinSupport(0.02)
  val minConfidence = 0.5

  val model = fpg.run(transactions)
  val freqItemSets =
    model.freqItemsets
      .filter(itemSet => itemSet.items.length > 1)
      .sortBy(-_.freq)
      .collect()

  freqItemSets.foreach { itemSet =>
    println(s"${itemSet.freq},${itemSet.items.sorted.mkString(",")}")
  }

  sc.stop()

}
