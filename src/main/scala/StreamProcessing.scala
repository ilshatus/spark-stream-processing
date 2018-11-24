import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.github.catalystcode.fortis.spark.streaming.rss.{RSSEntry, RSSInputDStream}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamProcessing {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\Ilshat\\hadoop")
    val durationSeconds = 10
    val conf = new SparkConf().setAppName("RSS Spark Application").setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(durationSeconds))
    sc.setLogLevel("ERROR")

    val urlCSV = args(0)
    val urls = urlCSV.split(",")
    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = durationSeconds)
    stream.foreachRDD(rdd => {
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      val entries = rdd.collect()
      entries.foreach((entry: RSSEntry) => {
        val description = entry
          .description
          .value
          .split("(<[^>]*>)|(((www\\.)|(https?:\\/\\/))[^ ]+)")
          .mkString(" ")
          .toLowerCase()
          .split("(([ \n\t\r\'\"!?@#$%^&*()_\\-+={}\\[\\]|<>;:,./`~\\\\])|(\\n)|(\\r))+")
          .filter(value => value.matches("[a-z]+"))
          .mkString(" ")
        if (!description.isEmpty)
          println(Tweet(entry.links.head.href, description))
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

case class Tweet(link: String, content: String) {
  override def toString: String = "Tweet(".concat(link).concat(", \"").concat(content).concat("\")")
}


