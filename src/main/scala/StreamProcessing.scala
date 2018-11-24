import StreamProcessing.TwitterEntity
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.github.catalystcode.fortis.spark.streaming.rss.{RSSEntry, RSSInputDStream}
import com.github.catalystcode.fortis.spark.streaming.html.HTMLInputDStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
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

     rdd.toDS()
      parseTweets(rdd.toDS())

    })

    // run forever
    ssc.start()
    ssc.awaitTermination()
  }
  def parseTweets(ds: Dataset[RSSEntry]): String ={
    var tweets:Array[TwitterEntity] = Array()
    ds.foreach((x:RSSEntry)=>
      {tweets= tweets ++(Array(new TwitterEntity(x.uri,x.description.value)))
      println(x.description.value+"\n___")
      })

    ""
  }
  class TwitterEntity(var url:String,var content:String,var author:String) {
    def this(uri:String,description: String){
      this("","","")
      val regex = "^>(.*)</a>$".r
      var seqe = Seq(description)
      val value = seqe.collect { case regex(a) => a.trim }
      val  textRegex = "^</a>(.*)$".r
      var text = seqe.collect { case textRegex(a) => a.trim }
      print(value)
      url = uri
      author =value.head

    }
  }

}


