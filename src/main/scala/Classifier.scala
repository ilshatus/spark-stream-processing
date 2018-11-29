import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


object Classifier {

  lazy val model: PipelineModel = PipelineModel.load(MODEL_FILE_NAME)

  private val APP_NAME = "Classificator"
  private val MASTER = "local"

  private val INPUT_FILE_PATH = "preprocessed_train.csv"
  private val LABEL_COLUMN = "label"
  private val TWEET_TEXT_COLUMN = "text"

  private val TF_COLUMN = "tf"
  private val IDF_COLUMN = "idf"
  private val WORDS_COLUMN = "words"

  private val MODEL_FILE_NAME = "tmp/tweets-classification-model"


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER)
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder()
      .appName(APP_NAME)
      .master(MASTER)
      .getOrCreate()

    // Prepare model
    val tokenizer = new Tokenizer()
      .setInputCol(TWEET_TEXT_COLUMN)
      .setOutputCol(WORDS_COLUMN)

    val hashingTF = new HashingTF()
      .setInputCol(WORDS_COLUMN)
      .setOutputCol(TF_COLUMN)

    val idf = new IDF()
      .setInputCol(TF_COLUMN)
      .setOutputCol(IDF_COLUMN)

    val svcModel = new LogisticRegression()
      .setFeaturesCol(IDF_COLUMN)
      .setLabelCol(LABEL_COLUMN)
      .setRegParam(0.005)
      .setMaxIter(300)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, idf, svcModel))

    // Load data
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

    val Array(training, test) = data.randomSplit(Array(0.7, 0.3), seed = 11L)


    // Fit the pipeline to training data.
    val model = pipeline.fit(training)

    testModel(model, test)

    // Save the fitted pipeline
    model.write.overwrite().save(MODEL_FILE_NAME)
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

  def predict(sample: String, spark: SparkSession, sc: SparkContext): Double = {
    // without this 'toDF' does not work
    import spark.implicits._

    val df = sc.parallelize(Seq(sample)).toDF("text")
    model.transform(df).select("probability").collect().apply(0).get(0).asInstanceOf[DenseVector].apply(1)
  }

}
