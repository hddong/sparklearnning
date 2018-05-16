package CoreTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Aggregate函数测试
  * aggregate[U](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
  *
  * note:
  *     zeroValue既参与 seqOp操作也参与 combOp操作
  */
object AggregateTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("AggregateTest")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val sc = spark.sparkContext
    println(test1(sc))
    println(test2(sc))
    println(test3(sc))
    println(test4(sc))
    /*
    8
    10
    24
    STARTSTARTabcabcdef
     */
  }

  // master local 默认单线程所以只有一个分区
  // 计算分区数的最大值
  // 最终最大值7和初始值0相加
  def test1(sc: SparkContext): Int = {
    val dataSet = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7))
    dataSet.aggregate(1)(math.max(_, _), _ + _)
  }

  // 分区为 Array(1, 2, 3) Array(4, 5, 6, 7)
  // 结果为 0+3+7 = 10
  def test2(sc: SparkContext): Int = {
    val dataSet = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7), 2)
    dataSet.aggregate(0)(math.max(_, _), _ + _)
  }

  // 分区为 Array(1, 2, 3) Array(4, 5, 6, 7)
  // 结果为 0+3+7 = 10
  def test3(sc: SparkContext): Int = {
    val dataSet = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7), 2)
    dataSet.aggregate(8)(math.max(_, _), _ + _)
  }

  def test4(sc: SparkContext): String = {
    val dataSet = sc.parallelize(Array("abc", "a", "b", "c", "d", "e", "f"))
    dataSet.aggregate("START")(_ + _, _ + _)
  }
//  def getSparkSession(): SparkSession = S
}
