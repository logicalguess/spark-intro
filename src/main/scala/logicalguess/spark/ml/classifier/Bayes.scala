package logicalguess.spark.ml.classifier

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object Bayes {

  def sparkJob() = {

    val conf = new SparkConf()
        .setAppName("Spark : classify mail as spam or non-spam with naive Bayes")
        .setMaster("local")

    val sc = new SparkContext(conf)

    // load the data and create RDD
    val data = sc.textFile("src/main/resources/data/spambase.csv")

    val parsedData = data.map { line =>
      val parts = line.split(',').map(_.toDouble)

      // Extract features for training as LabeledPoint
      // LabeledPoint is a couple (label, features)
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }

    // Split data into 2 sets : training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4))
    val training = splits(0).cache()
    val test = splits(1)

    // Training model on training set with the parameter lambda = 1
    val model = NaiveBayes.train(training, lambda = 1.0)

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println("accuracy " + accuracy)

    sc.stop()

  }

  def main(args: Array[String])= sparkJob()
}
