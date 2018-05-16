package DataFrameFunctionTest

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * function:
  *   VectorAssembler
  *   将多个列数据转换成 矩阵
  * time: 2018-2-11
  * @author hongdd
  */
object VectorAssemblerTest {
  def main(args: Array[String]): Unit = {
    // 初始化spark
    val spark = SparkSession.builder()
      .appName("VectorAssemblerTest")
      .master("local")
      .getOrCreate()

    // 创建出测试数据
    val dataSet = spark.createDataFrame(
      Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    // 数据转换
    // inputCols 待合并列
    // outCol 输出列名
    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(dataSet)
    output.show()

//    +---+----+------+--------------+-------+--------------------+
//    | id|hour|mobile|  userFeatures|clicked|            features|
//    +---+----+------+--------------+-------+--------------------+
//    |  0|  18|   1.0|[0.0,10.0,0.5]|    1.0|[18.0,1.0,0.0,10....|
//    +---+----+------+--------------+-------+--------------------+

    spark.stop()
  }
}
