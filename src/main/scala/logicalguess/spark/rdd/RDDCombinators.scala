package logicalguess.spark.rdd

import org.apache.spark.{SparkConf, SparkContext, SparkException}

import scala.collection.Iterator
import scala.collection.mutable.ListBuffer
import scala.io.StdIn

object RDDCombinators {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDCombinators").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // put some data in an RDD
    val letters = sc.parallelize('a' to 'z', 8)

    // another RDD of the same type
    val vowels = sc.parallelize(Seq('a', 'e', 'i', 'o', 'u'), 4)

    // subtract one from another, getting yet another RDD of the same type
    val consonants = letters.subtract(vowels)
    println("There are " + consonants.count() + " consonants")

    val vowelsNotLetters = vowels.subtract(letters)
    println("There are " + vowelsNotLetters.count() + " vowels that aren't letters")

    // union
    StdIn.readLine()
    val lettersAgain = consonants ++ vowels
    println("There really are " + lettersAgain.count() + " letters")

    // union with duplicates, removed
    StdIn.readLine()
    val tooManyVowels = vowels ++ vowels
    println("There aren't really " + tooManyVowels.count() + " vowels")
    val justVowels = tooManyVowels.distinct()
    println("There are actually " + justVowels.count() + " vowels")

    // subtraction with duplicates
    StdIn.readLine()
    val what = tooManyVowels.subtract(vowels)
    println("There are actually " + what.count() + " vowels left")

    // intersection
    StdIn.readLine()
    val earlyLetters = sc.parallelize('a' to 'l', 2)
    val earlyVowels = earlyLetters.intersection(vowels)
    println("The early vowels:")
    earlyVowels.foreach(println)

    // RDD of a different type
    val numbers = sc.parallelize(1 to 2, 2)

    // cartesian product
    StdIn.readLine()
    val cp = vowels.cartesian(numbers)
    println("Product has " + cp.count() + " elements")
    cp.foreach(println)

    // index the letters
    StdIn.readLine()
    val indexed = letters.zipWithIndex()
    println("indexed letters")
    indexed foreach {
      case (c, i) => println(i + ":  " + c)
    }
  }
}