import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.github.catalystcode.fortis.spark.streaming.rss.{RSSEntry, RSSInputDStream}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.math.min

object StreamProcessing {
  def main(args: Array[String]) {
    val durationSeconds = 60

    val conf = new SparkConf().setAppName("RSS Spark Application").
      setMaster("local[2]").
      set("spark.executor.memory","1g");


    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")

    val urlCSV = args(0)
    val urls = urlCSV.split(",")
    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds,readTimeout = 5000)
    stream.foreachRDD(rdd=>{
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      rdd.toDS().foreach(entry=>{

      val description = entry
        .description
        .value
        .toLowerCase
        .replaceAll("(<[^>]*>)", "")
        .split("(([ \n\t\r\'\"!?@#$%^&*()_\\-+={}\\[\\]|<>;:,./`~\\\\])|(\\n)|(\\r)|(((www\\.)|(https?:\\/\\/))[^ ]+))+")
        .filter(value => value.matches("[a-z]+"))
        .map(value => preprocess(value))
        .mkString(" ")
      println(Tweet(entry.links.head.href, description))}
      )
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
  def preprocess(word2: String): String ={

    val word = removeRedundantLetters(word2)
    var dictionary = scala.collection.mutable.HashMap[String, Boolean]()
    val sc = SparkContext.getOrCreate()
    sc.textFile("hdfs:///user/glasgow/dictionary.txt").collect().foreach(x => dictionary.put(x, true))
    if (dictionary.contains(word)) {
      word
    } else {
      var closestWord = ""
      var minDistance = 10000
      dictionary.foreach(tuple => {
        if (minDistance > levenshteinDistance(word, tuple._1)) {
          minDistance = levenshteinDistance(word, tuple._1)
          closestWord = tuple._1
        }
      })

      closestWord
    }
  }
  def levenshteinDistance[A](a: Iterable[A], b: Iterable[A]): Int =
    ((0 to b.size).toList /: a) ((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last



  def removeRedundantLetters(word: String): String ={
    val regex ="(\\b\\w*?)(\\w)\\2{2,}(\\w*)"
    word.replaceAll(regex,"$1$2$2$3")
  }
}

case class Tweet(link: String, content: String) {
  override def toString: String = "Tweet(".concat(link).concat(", \"").concat(content).concat("\")")
}


