package DataFrameFunctionTest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

/**
  * functions:
  *   monotonically_increasing_id: 单增长id -> 自增id函数
  *   其他方式: rdd zipWithIndex
  * time: 2018-2-1
  *
  * @author hongdd
  */
object RowidTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("RowIdTest").getOrCreate()
    
    val df = spark.createDataFrame(
      Array((1,"a"),(2,"b"),(3,"c"),(4,"b"),(5,"a"))).toDF("index","column")

    //调用函数
    df.withColumn("id", monotonically_increasing_id).show()
//  result
//    +-----+------+---+
//    |index|column| id|
//    +-----+------+---+
//    |    1|     a|  0|
//    |    2|     b|  1|
//    |    3|     c|  2|
//    |    4|     b|  3|
//    |    5|     a|  4|
//    +-----+------+---+
    spark.stop()
  }
}
