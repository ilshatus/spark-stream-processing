import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Classificator {
  private val APP_NAME = "Classificator"
  private val MASTER = "local"

  private val INPUT_FILE_PATH = "preprocessed_train.csv"
  private val LABEL_COLUMN = "label"
  private val TWEET_TEXT_COLUMN = "text"

  private val TF_COLUMN = "tf"
  private val IDF_COLUMN = "idf"
  private val WORDS_COLUMN = "words"

  private val MODEL_FILE_NAME = "/tmp/tweets-classification-model"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER)
    val sc = new SparkContext(conf)
    // $example on$F
    // Load and parse the data file.
    val spark: SparkSession = SparkSession.builder()
      .appName(APP_NAME)
      .master(MASTER)
      .getOrCreate()

    val data = spark.read
      .format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .schema(
        StructType(
          Array(
            StructField(LABEL_COLUMN, DoubleType, nullable = false),
            StructField(TWEET_TEXT_COLUMN, StringType, nullable = false)
          )
        )
      )
      .load(INPUT_FILE_PATH)
      //.limit(100)
      .cache()

    println(s"Count: ${data.count()}")
    // Training test split
    val Array(training, test) = data.randomSplit(Array(0.7, 0.3), seed = 11L)


    val tokenizer = new Tokenizer()
      .setInputCol(TWEET_TEXT_COLUMN)
      .setOutputCol(WORDS_COLUMN)

    val hashingTF = new HashingTF()
      .setInputCol(WORDS_COLUMN)
      .setOutputCol(TF_COLUMN)

    val idf = new IDF()
      .setInputCol(TF_COLUMN)
      .setOutputCol(IDF_COLUMN)

    val svcModel = new LinearSVC()
      .setFeaturesCol(IDF_COLUMN)
      .setLabelCol(LABEL_COLUMN)
      .setRegParam(0.0001)
      .setMaxIter(300)
    //.setUpdater(new SquaredL2Updater)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, idf, svcModel))

    // Fit the pipeline to training data.
    val model = pipeline.fit(training)

    testModel(model, test)

    // Save the fitted pipeline
    model.write.overwrite().save(MODEL_FILE_NAME)

    // Load the model
    val sameModel = PipelineModel.load(MODEL_FILE_NAME)

    testModel(sameModel, test)

    sc.stop()
  }

  private def testModel(model: PipelineModel, testData: Dataset[Row]): Unit = {
    // Make predictions on test documents.
    val testPredictions = model.transform(testData)

    val correctCount = testPredictions.select("prediction", LABEL_COLUMN)
      .collect()
      .count { case Row(prediction: Double, label: Double) => prediction.equals(label) }

    var accuracy = 1.0 * correctCount / testData.count()
    print(s"Accuracy: $accuracy")
  }
}
