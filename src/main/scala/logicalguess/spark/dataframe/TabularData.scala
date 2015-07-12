package logicalguess.spark.dataframe

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

object TabularData {

  case class Customer(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("DataFrame-SimpleCreation").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    def fromSchema(): DataFrame = {
      // create an RDD of Rows with some data
      val custs = Seq(
        Row(1, "Widget Co", 120000.00, 0.00, "AZ"),
        Row(2, "Acme Widgets", 410500.00, 500.00, "CA"),
        Row(3, "Widgetry", 410500.00, 200.00, "CA"),
        Row(4, "Widgets R Us", 410500.00, 0.0, "CA"),
        Row(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
      )
      val rows = sc.parallelize(custs, 4)

      val schema = StructType(
        Seq(
          StructField("id", IntegerType, true),
          StructField("name", StringType, true),
          StructField("sales", DoubleType, true),
          StructField("discount", DoubleType, true),
          StructField("state", StringType, true)
        )
      )

      // put the RDD[Row] and schema together to make a DataFrame
      sqlContext.createDataFrame(rows, schema)
    }

    def fromObjects(): DataFrame = {

      // create a sequence of case class objects
      val custs = Seq(
        Customer(1, "Widget Co", 120000.00, 0.00, "AZ"),
        Customer(2, "Acme Widgets", 410500.00, 500.00, "CA"),
        Customer(3, "Widgetry", 410500.00, 200.00, "CA"),
        Customer(4, "Widgets R Us", 410500.00, 0.0, "CA"),
        Customer(5, "Ye Olde Widgete", 500.00, 0.0, "MA")
      )
      val rows = sc.parallelize(custs, 4)

      // make it an RDD and convert to a DataFrame
      rows.toDF()
    }

    def fromTuples(): DataFrame = {
      // create an RDD of tuples with some data
      val custs = Seq(
        (1, "Widget Co", 120000.00, 0.00, "AZ"),
        (2, "Acme Widgets", 410500.00, 500.00, "CA"),
        (3, "Widgetry", 410500.00, 200.00, "CA"),
        (4, "Widgets R Us", 410500.00, 0.0, "CA"),
        (5, "Ye Olde Widgete", 500.00, 0.0, "MA")
      )
      val rows = sc.parallelize(custs, 4)

      // convert RDD of tuples to DataFrame by supplying column names
      val df = rows.toDF("id", "name", "sales", "discount", "state")
      df
    }

    val df = fromSchema()

    df.printSchema()

    // show all data
    StdIn.readLine()
    df.show()

    // show top few rows
    StdIn.readLine()
    df.limit(3).show()

    // select columns
    StdIn.readLine()
    df.select("sales", "state").show()

    // select columns with filter
    StdIn.readLine()
    df.filter($"state".equalTo("CA")).show()

    // computed columns
    StdIn.readLine()
    df.select(
      ($"sales" - $"discount").as("After Discount")).show()

    // add columns
    StdIn.readLine()
    df.select(
      df("*"),
      ($"sales" - $"discount").as("After Discount")).show()

    // aggregation
    StdIn.readLine()
    df.groupBy("state").agg("discount" -> "max").show()

    // column based aggregation
    StdIn.readLine()
    df.groupBy("state").agg(max($"discount")).show()

    // user defined aggregation function
    StdIn.readLine()

    def stddevFunc(c: Column): Column =
      sqrt(avg(c * c) - (avg(c) * avg(c)))

    df.groupBy("state").agg($"state", stddevFunc($"discount")).show()

    // shortcut for numeric columns
    StdIn.readLine()
    println("*** Aggregation short cuts")
    df.groupBy("state").count().show()
  }
}
