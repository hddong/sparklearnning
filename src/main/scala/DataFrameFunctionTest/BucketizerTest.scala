package DataFrameFunctionTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.SparkSession

/**
  * function:
  * Bucketizer 将连续数据离散化
  *    注：即将数据分桶
  * time: 2018-1-26
  *
  * @author hongdd
  */
object BucketizerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("bucketizerTest").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    // Double.NegativeInfinity 无穷小 Double.PositiveInfinity 无穷大
    // 将数据分到无穷小 -10  10  无穷大 三个区间中，下标为 0,1,2
    val splits = Array(Double.NegativeInfinity, -10, 10, Double.PositiveInfinity)

    val dataSet = Array(-30, -10, 5, 11, 30)
    val dataframe = spark.createDataFrame(dataSet.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer().setInputCol("features").setOutputCol("bucketedFeatures").setSplits(splits)
    val bucketedData = bucketizer.transform(dataframe)
    bucketedData.show()
//  result is:
//    |features|bucketedFeatures|
//    +--------+----------------+
//    |     -30|             0.0|
//    |     -10|             1.0|
//    |       5|             1.0|
//    |      11|             2.0|
//    |      30|             2.0|
//    +--------+----------------+
    spark.stop()

  }
}
