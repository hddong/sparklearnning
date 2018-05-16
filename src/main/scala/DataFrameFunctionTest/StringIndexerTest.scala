package DataFrameFunctionTest

import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
  * functions:
  *    StringIndexer 对String做映射产生index
  *    IndexToString 将映射的Index转回String
  *  time: 2018-2-1
  *
  *  @author hongdd
  */
object StringIndexerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("StringIndexerTest").getOrCreate()
    // 测试数据
    val df = spark.createDataFrame(
      Array((1,"a"),(2,"b"),(3,"c"),(4,"b"),(5,"a"))).toDF("index","column")
    // 建立转化器,指定输入和输出
    val indexer = new StringIndexer()
      .setInputCol("column")
      .setOutputCol("categoryIndex")
    val indexed = indexer.fit(df).transform(df)
    indexed.show()
//  result
//    +-----+------+-------------+
//    |index|column|categoryIndex|
//    +-----+------+-------------+
//    |    1|     a|          0.0|
//    |    2|     b|          1.0|
//    |    3|     c|          2.0|
//    |    4|     b|          1.0|
//    |    5|     a|          0.0|
//    +-----+------+-------------+
    // 将转化完的indexed转回之前状态
    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")
    val converted = converter.transform(indexed)
    converted.show()
//  result
//    +-----+------+-------------+----------------+
//    |index|column|categoryIndex|originalCategory|
//    +-----+------+-------------+----------------+
//    |    1|     a|          0.0|               a|
//    |    2|     b|          1.0|               b|
//    |    3|     c|          2.0|               c|
//    |    4|     b|          1.0|               b|
//    |    5|     a|          0.0|               a|
//    +-----+------+-------------+----------------+

    spark.stop()
  }
}
