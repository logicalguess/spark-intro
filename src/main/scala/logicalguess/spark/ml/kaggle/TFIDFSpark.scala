package logicalguess.spark.ml.kaggle

import edu.stanford.nlp.util.logging.RedwoodConfiguration
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object TFIDFSpark {

  final val CSVSeparator = '\t'
  final val path = "./src/main/resources/data/labeledTrainData.tsv"

  def main(args: Array[String]) = {
    RedwoodConfiguration.empty().capture(Console.err).apply();

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TfIdfSpark")
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    val input: RDD[String] = sc.textFile(path).sample(false, 0.2)
    val data = SparkUtils.labeledTexts(input, CSVSeparator)

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val validation = splits(1)

    //create features
    val X_train = SparkUtils.tfidfTransformer(training).cache()
    val X_test = SparkUtils.tfidfTransformer(validation).cache()

    //Train / Predict
    //val model = NaiveBayes.train(X_train,lambda = 1.0)
    //val model = SVMWithSGD.train(X_train, 100)
    val model = LogisticRegressionWithSGD.train(X_train, 100)

    val predictionAndLabels = X_test.map(x => (model.predict(x.features), x.label))

    SparkUtils.evaluateModel(predictionAndLabels, "Results")
  }
}
