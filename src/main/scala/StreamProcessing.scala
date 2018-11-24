import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.github.catalystcode.fortis.spark.streaming.rss.RSSInputDStream
import com.github.catalystcode.fortis.spark.streaming.html.HTMLInputDStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamProcessing {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\arsee\\Desktop\\hadoop")
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
    stream.foreachRDD(rdd=>{
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      rdd.toDS().show()
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
}


