package CoreTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object WordCountTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("AggregateTest")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = spark.sparkContext
    println(test1(sc))
    println(test2(sc))
    println(test3(sc).take(10).mkString(", "))

    spark.stop()
  }

  def test1(sc: SparkContext) = {
    val dataSet = sc.parallelize(Array("hello", "world", "hello", "everyone"))
    dataSet.countByValue()
  }
  def test2(sc: SparkContext) = {
    val dataSet = sc.parallelize(Array("hello", "world", "hello", "everyone"))
    dataSet.map((_, 1)).countByKey()
  }
  def test3(sc: SparkContext) = {
    val dataSet = sc.parallelize(Array("hello", "world", "hello", "everyone"))
    dataSet.map((_, 1)).reduceByKey(_ + _)
  }
}
