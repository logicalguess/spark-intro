package logicalguess.spark.ml.recommender

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

/**
  * Created by logicalguess on 1/2/16.
  */
object MovieRecommender {

  def main(args: Array[String]) = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SparkRecommender").setMaster("local[2]").set("spark.executor.memory", "4g")
    val sc = new SparkContext(conf)

    /* Load the raw ratings data from a file */
    val rawData = sc.textFile("src/main/resources/ml-100k/u.data")
    println(rawData.first())

    /* Extract the user id, movie id and rating only from the dataset */
    val rawRatings = rawData.map(_.split("\t").take(3))

    /* Construct the RDD of Rating objects */
    val ratings = rawRatings.map {
      case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toDouble)
    }
    println(ratings.first())

    /* Train the ALS model with rank=50, iterations=10, lambda=0.01 */
    val model = ALS.train(ratings, 50, 10, 0.01)

    /* Count user factors and force computation */
    println(model.userFeatures.count)

    println(model.productFeatures.count)


    val userId = 789
    val movieId = 123

    /* Make a prediction for a single user and movie pair */
    var predictedRating = model.predict(userId, movieId)

    /* Make predictions for a single user across all movies */
    var K = 10
    val topKRecs = model.recommendProducts(userId, K)
    println(topKRecs.mkString("\n"))

    /* Load movie titles to inspect the recommendations */
    val movies = sc.textFile("src/main/resources//ml-100k/u.item")
    val pairRDD: RDD[(Int, String)] = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt, array(1)))
    val titles = pairRDD.collectAsMap()
    println(titles(movieId))

    val moviesForUser = ratings.keyBy(_.user).lookup(userId)
    println(moviesForUser.size)

    moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)
    topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)

    /* Compute the cosine similarity between two vectors */
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }

    // cosineSimilarity: (vec1: org.jblas.DoubleMatrix, vec2: org.jblas.DoubleMatrix)Double
    val itemId = 567
    val itemFactor = model.productFeatures.lookup(itemId).head

    val itemVector = new DoubleMatrix(itemFactor)

    val sims = model.productFeatures.map { case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }
    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    println(sortedSims.mkString("\n"))

    /* We can check the movie title of our chosen movie and the most similar movies to it */
    println(titles(itemId))

    val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    sortedSims2.slice(1, 11).map { case (id, sim) => (titles(id), sim) }.mkString("\n")

    /* Compute squared error between a predicted and actual rating */
    val actualRating = moviesForUser.take(1)(0)
    predictedRating = model.predict(userId, actualRating.product)
    val squaredError = math.pow(predictedRating - actualRating.rating, 2.0)

    /* Compute Mean Squared Error across the dataset */
    // Below code is taken from the Apache Spark MLlib guide at: http://spark.apache.org/docs/latest/mllib-guide.html#collaborative-filtering-1
    val usersProducts = ratings.map { case Rating(user, product, rating) => (user, product) }
    val predictions = model.predict(usersProducts).map {
      case Rating(user, product, rating) => ((user, product), rating)
    }
    val ratingsAndPredictions = ratings.map {
      case Rating(user, product, rating) => ((user, product), rating)
    }.join(predictions)
    val MSE = ratingsAndPredictions.map {
      case ((user, product), (actual, predicted)) => math.pow((actual - predicted), 2)
    }.reduce(_ + _) / ratingsAndPredictions.count
    println("Mean Squared Error = " + MSE)

    val RMSE = math.sqrt(MSE)
    println("Root Mean Squared Error = " + RMSE)

    /* Compute Mean Average Precision at K */

    /* Function to compute average precision given a set of actual and predicted ratings */
    // Code for this function is based on: https://github.com/benhamner/Metrics
    def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
      val predK = predicted.take(k)
      var score = 0.0
      var numHits = 0.0
      for ((p, i) <- predK.zipWithIndex) {
        if (actual.contains(p)) {
          numHits += 1.0
          score += numHits / (i.toDouble + 1.0)
        }
      }
      if (actual.isEmpty) {
        1.0
      } else {
        score / scala.math.min(actual.size, k).toDouble
      }
    }

    val actualMovies = moviesForUser.map(_.product)
    val predictedMovies = topKRecs.map(_.product)
    val apk10 = avgPrecisionK(actualMovies, predictedMovies, 10)

    /* Compute recommendations for all users */
    val itemFactors = model.productFeatures.map { case (id, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows, itemMatrix.columns)

    // broadcast the item factor matrix
    val imBroadcast = sc.broadcast(itemMatrix)

    // compute recommendations for each user, and sort them in order of score so that the actual input
    // for the APK computation will be correct
    val allRecs = model.userFeatures.map { case (userId, array) =>
      val userVector = new DoubleMatrix(array)
      val scores = imBroadcast.value.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendedIds = sortedWithId.map(_._2 + 1).toSeq
      (userId, recommendedIds)
    }

    // next get all the movie ids per user, grouped by user id
    val userMovies = ratings.map { case Rating(user, product, rating) => (user, product) }.groupBy(_._1)

    // finally, compute the APK for each user, and average them to find MAPK
    K = 10
    val MAPK = allRecs.join(userMovies).map { case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, K)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision at K = " + MAPK)

    /* Using MLlib built-in metrics */

    // MSE, RMSE and MAE
    val predictedAndTrue = ratingsAndPredictions.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
    // Mean Squared Error = 0.08231947642632852
    // Root Mean Squared Error = 0.2869137090247319

    // MAPK
    val predictedAndTrueForRanking = allRecs.join(userMovies).map { case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2)
      (predicted.toArray, actual.toArray)
    }
    val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
    println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)

    // Compare to our implementation, using K = 2000 to approximate the overall MAP
    val MAPK2000 = allRecs.join(userMovies).map { case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, 2000)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision = " + MAPK2000)
  }

}
