import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession}

object ClassifierUsageExample {

  private val APP_NAME = "ClassificatorUsageExample"
  private val MASTER = "local"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER)
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder()
      .appName(APP_NAME)
      .master(MASTER)
      .getOrCreate()

    print(Classifier.predict("Some lovely text from twitter", spark, sc))
  }
}
