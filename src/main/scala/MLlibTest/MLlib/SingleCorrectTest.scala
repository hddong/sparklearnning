package MLlibTest.MLlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SingleCorrectTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SingleCorrectTest")
      .master("local")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    val sc = spark.sparkContext
    val seriesX: RDD[Double] = sc.parallelize(Array(1.0, 10.0, 100.0, 12.0))  // a series
    // must have the same number of partitions and cardinality as seriesX
    val seriesY: RDD[Double] = sc.parallelize(Array(2.0, 20.0, 200.0, 15.0))

    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
    // method is not specified, Pearson's method will be used by default.
    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    println(s"Correlation is: $correlation")

    val data = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0, 12.0),
        Vectors.dense(2.0, 20.0, 200.0, 15.0),
        Vectors.dense(5.0, 33.0, 366.0, 20.0),
        Vectors.dense(4.0, 55.0, 245.0, 40.0))
    )  // note that each Vector is a row and not a column

    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
    // If a method is not specified, Pearson's method will be used by default.
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")

    println(correlMatrix.toString)
  }
}
