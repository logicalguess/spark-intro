package logicalguess.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

object WordCount {

  def sparkJob() = {

    val conf = new SparkConf()
                  .setAppName("Spark word count")
                  .setMaster("local")

    val sc = new SparkContext(conf)

    // Load the data and create RDD
    val data = sc.textFile("src/main/resources/wordcount.txt")

    val wordCounts = data.flatMap(line => line.split("\\s+")) // parse data
                         .map(word => (word, 1)) // map step
                         .reduceByKey(_ + _) // reduce step

    // Persist data
    wordCounts.cache()

    println(wordCounts.collect().toList)

    // Print 10
    StdIn.readLine()
    println(wordCounts.take(10).toList)

    // Keep words which appear more than 20 times
    StdIn.readLine()
    val filteredWordCount = wordCounts.filter{
      case (key, value) => value > 20
    }

    filteredWordCount.count()

    println(filteredWordCount.collect().toList)

    // Make count the key
    StdIn.readLine()
    val reversedCounts = filteredWordCount.map(pair => (pair._2, pair._1))
                            .sortByKey(false)

    println(reversedCounts.collect().toList)

    sc.stop()
  }

  def main(args: Array[String])= sparkJob()

}
