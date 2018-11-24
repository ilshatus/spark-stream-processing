import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.util.MLUtils
// $example off$

object Classificator {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Classificator").setMaster("local")
    val sc = new SparkContext(conf)
    // $example on$F
    // Load and parse the data file.

    val documents: RDD[Seq[String]] = sc.textFile("preprocessed_train.txt")
      .map(_.split(",")(1).split(" ").toSeq)

    val labels: RDD[Int] = sc.textFile("preprocessed_train.txt").map(_.split(",")(0).toInt)

    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)

    // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    // First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()

    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    // spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
    // a minimum number of documents. In such cases, the IDF for these terms is set to 0.
    // This feature can be used by passing the minDocFreq value to the IDF constructor.
    //    val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
    //    val tfidfIgnore: RDD[linalg.Vector] = idfIgnore.transform(tf)

    val data = tfidf.zip(labels).map(t => LabeledPoint(t._2.toDouble, t._1))


    //    val d = tfidf.map(line => LabeledPoint(line.toJson, [line.toArray.take(1)] )) // arbitrary mapping, it's just an example

    //    tfidf.foreach(v => )

    //val data = MLUtils.loadLibSVMFile(sc, "sample_lib_smv_data.csv")

    //    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.7, 0.3))
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    var predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    var accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    print(s"Accuracy: $accuracy")

    // Save and load model
    model.save(sc, "target/tmp/myNaiveBayesModel")
/*
    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
    // $example off$

    predictionAndLabel = test.map(p => (sameModel.predict(p.features), p.label))
    accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    print(accuracy)
*/

    sc.stop()
  }
}
