import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


class SVMClassifier(filepath: String) {

  private var data: RDD[LabeledPoint] = _
  private var model: SVMModel = _

  def init() = {
    val conf = new SparkConf().setAppName("SVMClassifier").setMaster("local")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .appName("SVMClassifier")
      .master("local")
      .getOrCreate()

    val file = sc.textFile(filepath)

    val documents = file.map(_.split(", ")(1).split(" ").toSeq)

    val labels = file.map(_.split(", ")(0).toInt).collect()

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(documents.map(Tuple1.apply)).toDF("document")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("document")
      .setOutputCol("vector")
      .setVectorSize(10)
      .setMinCount(0)
    val word2VecModel = word2Vec.fit(documentDF)

    val result = word2VecModel.transform(documentDF)

    var i = -1
    def index(): Int = {
      i += 1
      i
    }

    data = result.rdd.map { row =>
      LabeledPoint(labels(index()), Vectors.fromML(row.getAs[org.apache.spark.ml.linalg.SparseVector]("vector").toDense))
    }

    model = fit()
  }

  def fit(): SVMModel = {

    // Training test split
//    val Array(training, test) = data.randomSplit(Array(0.7, 0.3), seed = 11L)

    val numIterations = 300
    val regParam = 0.0001

    val svmModel = new SVMWithSGD()
    svmModel.optimizer
      .setNumIterations(numIterations)
      .setUpdater(new SquaredL2Updater)
      .setRegParam(regParam)

    // Run training algorithm to build the model

    svmModel.run(data)

    // Uncomment only when need to know accuracy
    // // Clear the default threshold.
//    model.clearThreshold()

    // Compute raw scores on the test set.
//    val scoreAndLabels = test.map { point =>
//      val score = model.predict(point.features)
//      (score, point.label)
//    }
//
//    // Get evaluation metrics.
//    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
//    val auROC = metrics.areaUnderROC()
//
//    println("Accuracy = " + auROC)
  }

  def predict(features: Vector): Double = {
    model.predict(features)
  }
}