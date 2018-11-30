import java.io.PrintWriter

import breeze.linalg.min
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.github.catalystcode.fortis.spark.streaming.rss.{RSSEntry, RSSInputDStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamProcessing {

  private val APP_NAME = "Stream processing"

  // Polling period of RSS
  private val DURATION_IN_SECONDS = 10

  // File with dictionary of english words
  private val DICTIONARY_FILE = "hdfs:///user/glasgow/dictionary.txt"

  // Name of file, which will contain result of processing
  private val PROCESSING_RESULTS = "hdfs:///user/glasgow/processing_results.txt"

  // Directory with trained model
  private val MODEL_DIRECTORY = "hdfs:///user/glasgow/tmp/tweets-classification-model"

  def main(args: Array[String]) {

    // Initialize spark context
    val conf = new SparkConf().setAppName(APP_NAME)
      .setMaster("local[*]")
      .set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    // Read dictionary of english words
    val dictionary = scala.collection.mutable.HashMap[String, Boolean]()

    // When run on local machine
    sc.textFile(DICTIONARY_FILE).collect().foreach(x => dictionary.put(x, true))
    // Store in shared broadcast variable
    val commonDictionary = sc.broadcast(dictionary)

    // Initialize stream context
    val ssc = new StreamingContext(sc, Seconds(DURATION_IN_SECONDS))

    sc.setLogLevel("ERROR")
    if (args.length == 0) {
      println("You should provide link to RSS feed")
      return
    }

    val urlCSV = args(0)
    val urls = urlCSV.split(",")
    // Initialize rss feed stream
    val stream = new RSSInputDStream(urls, Map[String, String](
      "User-Agent" -> "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
    ), ssc, StorageLevel.MEMORY_ONLY, pollingPeriodInSeconds = DURATION_IN_SECONDS, readTimeout = 5000)

    // Create new file, where we will write processing results
    val hdfs = FileSystem.get(new Configuration())
    hdfs.create(new Path(PROCESSING_RESULTS), true).close()
    hdfs.close()

    // Load model from directory and store in shared broadcast variable
    val model = PipelineModel.load(MODEL_DIRECTORY)
    val modelBC = sc.broadcast(model)

    // Will call given url for rss feed in polling period
    stream.foreachRDD((rdd, time) => {

      println(time)

      // Process each tweet, which appear after previous poll
      rdd.foreach((entry: RSSEntry) => {

        val description = entry
          .description
          .value
          .toLowerCase // map text to lower case
          .replaceAll("(<[^>]*>)", "") // remove all html tags
          // split text by symbols in following regular expression + remove links
          .split("(([ \n\t\r\'\"!?@#$%^&*()_\\-+={}\\[\\]|<>;:,./`~\\\\])|(\\n)|(\\r)|(((www\\.)|(https?:\\/\\/))[^ ]+))+")
          .filter(value => value.matches("[a-z]+")) // remove all words, which contains non english letters
          .map(value => preprocess(value, commonDictionary)) // preprocess each word
          .mkString(" ") // merge words to text by whitespaces

        // skip eventually empty tweets
        if (!description.isEmpty) {

          val spark = SparkSession.builder().appName(APP_NAME).getOrCreate()
          import spark.implicits._
          val df = spark.createDataset(Seq(description)).toDF("text")

          // probability that tweet is positive
          val proba = modelBC.value.transform(df)
            .select("probability").collect().apply(0)
            .get(0).asInstanceOf[DenseVector].apply(1)

          var tweet = ""
          if (proba > .5) { // if probability is more than 50%, consider it like positive
            tweet = "{Positive}(" + proba + ") "
          } else { // otherwise consider as negative
            tweet = "{Negative}(" + proba + ") "
          }

          tweet += Tweet(entry.links.head.href, description).toString

          // Print result to terminal and to file
          println(tweet)

          val hdfs = FileSystem.get(new Configuration())
          val output = hdfs.append(new Path(PROCESSING_RESULTS))
          val writer = new PrintWriter(output)
          writer.println(tweet)
          writer.close()
          output.close()
          hdfs.close()
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * Function which will map given word to word in dictionary,
    * which has the smallest lvenshtein distance to given word.
    * If word contains more than 2 consequent letters, they will be removed
    *
    * @param word2
    * @param commonDictionary
    * @return
    */
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

  /**
    * Function which computes levenshtein distance of two words
    *
    * @param a
    * @param b
    * @tparam A
    * @return
    */
  def levenshteinDistance[A](a: Iterable[A], b: Iterable[A]): Int =
    ((0 to b.size).toList /: a) ((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }) last

  /**
    * Function which will remove redundant letters
    * if word contains more than 2 consequent letters
    *
    * @param word
    * @return
    */
  def removeRedundantLetters(word: String): String = {
    val regex = "(\\b\\w*?)(\\w)\\2{2,}(\\w*)"
    word.replaceAll(regex, "$1$2$2$3")
  }
}

case class Tweet(link: String, content: String) {
  override def toString: String = "Tweet(".concat(link).concat(", \"").concat(content).concat("\")")
}