import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.github.catalystcode.fortis.spark.streaming.html.HTMLInputDStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamProcessing {

  def main(args: Array[String]) {
    val duration = 10

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val log = Logger.getLogger("StreamProcessing")

    log.info("Args parsed.")
    val conf = new SparkConf()
      .setAppName("StreamProcessing")
    val sc = new SparkContext(conf)
    log.info("Context ready.")

    val ssc = new StreamingContext(sc, Seconds(duration))
    log.info("Stream context is ready")

    if (args.length<1){
      log.info("You have to pass at least 1 argument")
      return -1
    }
    val urlCSV = args(0)
    val urls = urlCSV.split(",")
    val stream = new HTMLInputDStream(
      urls,
      ssc,
      requestHeaders = Map[String, String](
        "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
      )
    )
    stream.foreachRDD(rdd=>{
      val spark = SparkSession.builder().appName(sc.appName).getOrCreate()
      import spark.sqlContext.implicits._
      rdd.toDS().show(100)
    })

    // run forever
    ssc.start()
    ssc.awaitTermination()

  }
}


