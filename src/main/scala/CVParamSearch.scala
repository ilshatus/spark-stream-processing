import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object CVParamSearch {

  private val APP_NAME = "Classificator"
  private val MASTER = "local"

  private val INPUT_FILE_PATH = "preprocessed_train.csv"
  private val LABEL_COLUMN = "label"
  private val TWEET_TEXT_COLUMN = "text"

  private val TF_COLUMN = "tf"
  private val IDF_COLUMN = "idf"
  private val WORDS_COLUMN = "words"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER)
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder()
      .appName(APP_NAME)
      .master(MASTER)
      .getOrCreate()


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
      .limit(50000)
      .cache()

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

    val svcModel = new LinearSVC()
      .setFeaturesCol(IDF_COLUMN)
      .setLabelCol(LABEL_COLUMN)
      .setRegParam(0.01)
      .setMaxIter(300)

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, idf, svcModel))

    val paramGrid = new ParamGridBuilder()
      //.addGrid(svcModel.regParam, Array(0.01))
      //.addGrid(svcModel.maxIter, Array(50, 100, 300))
      .addGrid(svcModel.tol, Array(0, 0.0001, 0.001, 0.01, 0.1))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val cvModel = cv.fit(data)

    println("Best params:")
    println(cvModel.bestModel)

    val bestPipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    val stages = bestPipelineModel.stages

    val svcStage = stages(3).asInstanceOf[LinearSVCModel]
    println("regParam = " + svcStage.getRegParam)
    println("maxIter = " + svcStage.getMaxIter)
    println("tol = " + svcStage.getTol)
  }
}
