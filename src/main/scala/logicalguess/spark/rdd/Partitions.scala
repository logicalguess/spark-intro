package logicalguess.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{Iterator, mutable}
import scala.io.StdIn

object Partitions {

  // create an easy way to look at the partitioning of an RDD
  def analyze[T](r: RDD[T]) : Unit = {
    val partitions = r.glom()
    println(partitions.count() + " partitions")

    // use zipWithIndex() to see the index of each partition
    // we need to loop sequentially so we can see them in order: use collect()
    partitions.zipWithIndex().collect().foreach {
      case (a, i) => {
        println("Partition " + i + " contents:" +
          a.foldLeft("")((e, s) => e + " " + s))
      }
    }
    println()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Partitions").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // look at the distribution of numbers across partitions
    val numbers =  sc.parallelize(1 to 100, 4)
    println("original RDD:")
    analyze(numbers)

    // filter
    StdIn.readLine()
    val some = numbers.filter(_ < 34)
    println("filtered RDD")
    analyze(some)

    // subtract
    StdIn.readLine()
    val diff = numbers.subtract(some)
    println("the complement:")
    analyze(diff)
    println("it is a " + diff.getClass.getCanonicalName)

    // setting the number of partitions doesn't help (it was right anyway)
    StdIn.readLine()
    val diffSamePart = numbers.subtract(some, 4)
    println("the complement (explicit but same number of partitions):")
    analyze(diffSamePart)

    // we can change the number but it also doesn't help
    StdIn.readLine()
    val diffMorePart = numbers.subtract(some, 6)
    println("the complement (different number of partitions):")
    analyze(diffMorePart)
    println("it is a " + diffMorePart.getClass.getCanonicalName)

    // repartition
    StdIn.readLine()
    val threePart = numbers.repartition(3)
    println("numbers in three partitions")
    analyze(threePart)
    println("it is a " + threePart.getClass.getCanonicalName)

    // a ShuffledRDD with interesting characteristics
    StdIn.readLine()
    val groupedNumbers = numbers.groupBy(n => if (n % 2 == 0) "even" else "odd")
    println("numbers grouped into 'odd' and 'even'")
    analyze(groupedNumbers)
    println("it is a " + groupedNumbers.getClass.getCanonicalName)

  }
}
