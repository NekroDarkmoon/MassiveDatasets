package stackoverflow

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.functions.udf
import org.apache.log4j.{Level, Logger}

object Topics extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("MinHash")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR") // avoid all those messages going on
  val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

  val filename = args(0)

  // remove line below and replace with your code
  println("Reading from file ", filename)

  // Get Input files
  val langFile = Source.fromFile("data/languages.csv").getLines().drop(1)
  val dataset = spark.read.format("libsvm").load(filename)

  // Create languages index
  val languages: Map[Int, String] =
    langFile.foldLeft(Map[Int, String]()) { (acc, lang) =>
      val str = lang.trim.split(",").toList

      acc + (str.head.toInt -> str.last)
    }

  // LDA model
  println("Model fitting...")
  val lda = new LDA().setSeed(0).setK(25).setMaxIter(20)
  val model = lda.fit(dataset)

  println("Topic Description")

  val topics = model.describeTopics().collect().toList

  topics.foreach(topic => {
    println(s"\nCluster ${topic.get(0)}\n")

    val words: List[(Int, Double)] =
      topic.getSeq(1).zip(topic.getSeq(2)).toList

    val langs = words
      .filter(w => w._2 >= 0.05)
      .map(w => {

        s"${languages.getOrElse(w._1, "zzz")}, ${w._2}"
      })

    println(langs.mkString("\n"))
  })

  sc.stop()

}
