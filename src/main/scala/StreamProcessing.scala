import java.io.{File, FileOutputStream, PrintWriter}

import breeze.linalg.min
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.github.catalystcode.fortis.spark.streaming.rss.{RSSEntry, RSSInputDStream}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamProcessing {

  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\Users\\Ilshat\\hadoop")
    val durationSeconds = 30
    val conf = new SparkConf().setAppName("RSS Spark Application")
      .setIfMissing("spark.master", "local[*]")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val dictionary = scala.collection.mutable.HashMap[String, Boolean]()
    //sc.textFile("hdfs:///user/glasgow/dictionary.txt").collect().foreach(x => dictionary.put(x, true))
    sc.textFile("dictionary.txt").collect().foreach(x => dictionary.put(x, true))
    val commonDictionary = sc.broadcast(dictionary)

    val ssc = new StreamingContext(sc, Seconds(durationSeconds))

    sc.setLogLevel("ERROR")

    val urlCSV = args(0)
    val urls = urlCSV.split(",")
    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds, readTimeout = 5000)

    stream.foreachRDD((rdd, time) => {
      println(time)

      rdd.foreach((entry: RSSEntry) => {
        val description = entry
          .description
          .value
          .toLowerCase
          .replaceAll("(<[^>]*>)", "")
          .split("(([ \n\t\r\'\"!?@#$%^&*()_\\-+={}\\[\\]|<>;:,./`~\\\\])|(\\n)|(\\r)|(((www\\.)|(https?:\\/\\/))[^ ]+))+")
          .filter(value => value.matches("[a-z]+"))
          .map(value => preprocess(value, commonDictionary))
          .mkString(" ")
        if (!description.isEmpty) {
          val proba = Classifier.predict(description)
          var tweet = ""
          if (proba > .5) {
            tweet = "{Positive}(" + proba + ") "
          } else {
            tweet = "{Negative}(" + proba + ") "
          }
          tweet += Tweet(entry.links.head.href, description).toString
          println(tweet)
          val p = new PrintWriter(new FileOutputStream(new File("anime.txt"), true))
          p.println(tweet)
          p.flush()
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }


  def preprocess(word2: String, commonDictionary: Broadcast[scala.collection.mutable.HashMap[String, Boolean]]): String = {

    val word = removeRedundantLetters(word2)

    if (commonDictionary.value.contains(word)) {
      word
    } else {
      var closestWord = ""
      var minDistance = 10000
      commonDictionary.value.foreach(tuple => {
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


  def removeRedundantLetters(word: String): String = {
    val regex = "(\\b\\w*?)(\\w)\\2{2,}(\\w*)"
    word.replaceAll(regex, "$1$2$2$3")
  }
}

case class Tweet(link: String, content: String) {
  override def toString: String = "Tweet(".concat(link).concat(", \"").concat(content).concat("\")")
}