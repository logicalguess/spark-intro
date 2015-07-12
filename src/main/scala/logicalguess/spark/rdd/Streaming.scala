package logicalguess.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//
// A utility for streaming data through an in-memory queue
//
class QueueMaker(sc: SparkContext, ssc:StreamingContext) {

  private val rddQueue = new mutable.Queue[RDD[Int]]()

  val inputStream = ssc.queueStream(rddQueue)

  private var base = 1

  // each RDD has 100 different integers
  private def makeRDD() : RDD[Int] = {
    val rdd = sc.parallelize(base to base + 99 , 4)
    base = base + 100
    rdd
  }

  // put 10 RDDs in the queue
  def populateQueue() : Unit = {
    for (n <- 1 to 10) {
      rddQueue.enqueue(makeRDD())
    }
  }
}

object Streaming {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("QueueBasedStreaming").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))
    val qm = new QueueMaker(sc, ssc)

    // create the stream
    val stream = qm.inputStream

    // register for data
    stream.foreachRDD(r => {
      println(r.count())
    })

    // register for data -- a five second sliding window every two seconds
    stream.window(Seconds(5), Seconds(2)).foreachRDD(r => {
      if (r.count() == 0)
        println("Empty")
      else
        println("Count = " + r.count() + " min = " + r.min()+ " max = " + r.max())
    })


    // start streaming
    ssc.start()

    // start producing data
    qm.populateQueue()

    while (true) {
      Thread.sleep(100)
    }
  }
}
