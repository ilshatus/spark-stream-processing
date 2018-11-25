import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object Classificator {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Classificator").setMaster("local")
    val sc = new SparkContext(conf)

    val documents: RDD[Seq[String]] = sc.textFile("preprocessed_train.txt")
      .map(_.split(",")(1).split(" ").toSeq)

    val labels: RDD[Int] = sc.textFile("preprocessed_train.txt").map(_.split(",")(0).toInt)

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    tf.cache()

    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val data = tfidf.zip(labels).map(t => LabeledPoint(t._2.toDouble, t._1))

    // Training test split
    val Array(training, test) = data.randomSplit(Array(0.7, 0.3), seed = 11L)

    val numIterations = 300
    val regParam = 0.0001

    val svmModel = new SVMWithSGD()
    svmModel.optimizer
      .setNumIterations(numIterations)
      .setUpdater(new SquaredL2Updater)
      .setRegParam(regParam)

    // Run training algorithm to build the model
    val model = svmModel.run(training)

    // Uncomment only when need to know accuracy
    // // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }
    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Accuracy = " + auROC)

    sc.stop()
  }

}
