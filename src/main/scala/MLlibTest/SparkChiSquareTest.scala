package MLlibTest

import org.apache.spark.ml.feature.{ChiSqSelector, VectorAssembler}
import org.apache.spark.sql.SparkSession

/**
  * function:
  *     假设检验 特征选择
  *     卡方检验
  * time: 2018-2-11
  * @author hongdd
  */
object SparkChiSquareTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("chisquareTest")
      .master("local")
      .getOrCreate()
    // 读取csv文件
    // options:
    //    header true表示文件包含头文件
    //    inferSchema true 自动确定数据类型
    val data = spark.read
      .option("header","false")
      .option("inferSchema","true")
      .csv("./src/main/resources/MLlibData/tmp_matrix_input_VVPirtNtVKxRxnt.log")
      .toDF("date_str", "grid_num", "crime_num", "crime_bi", "same_crime",
        "period0_temporal_spatial_0_0", "period0_temporal_spatial_0_1",
        "period0_temporal_spatial_1_0", "period0_temporal_spatial_1_1",
        "period0_temporal_spatial_2_0", "period0_temporal_spatial_2_1",
        "period0_temporal_spatial_3_0", "period0_temporal_spatial_3_1",
        "period0_temporal_spatial_4_0", "period0_temporal_spatial_4_1")
    data.show()
    val colArray = Array("date_str", "grid_num", "crime_num", "crime_bi", "same_crime",
      "period0_temporal_spatial_0_0", "period0_temporal_spatial_0_1",
      "period0_temporal_spatial_1_0", "period0_temporal_spatial_1_1",
      "period0_temporal_spatial_2_0", "period0_temporal_spatial_2_1",
      "period0_temporal_spatial_3_0", "period0_temporal_spatial_3_1",
      "period0_temporal_spatial_4_0", "period0_temporal_spatial_4_1")
    val colArray2 = Array("same_crime",
      "period0_temporal_spatial_0_0", "period0_temporal_spatial_0_1",
      "period0_temporal_spatial_1_0", "period0_temporal_spatial_1_1",
      "period0_temporal_spatial_2_0", "period0_temporal_spatial_2_1",
      "period0_temporal_spatial_3_0", "period0_temporal_spatial_3_1",
      "period0_temporal_spatial_4_0", "period0_temporal_spatial_4_1")
    // 将多个列数据转换成 矩阵
    val vecDF = new VectorAssembler().setInputCols(colArray2).setOutputCol("features").transform(data)

//    val lrModel = new LogisticRegression().setLabelCol("crime_bi").setFeaturesCol("features").fit(vecDF)
    // 使用卡方选择特征
    val result = new ChiSqSelector()
      .setFeaturesCol("features")
      .setLabelCol("crime_bi")
      .setOutputCol("selectCol")
      .setFpr(0.1)
      .fit(vecDF)
      .transform(vecDF)
    result.show()
//    val result = ChiSquareTest.test(vecDF, "features", "crime_bi")
//    result.select("pValues").rdd.map(row => row.get(0) ).foreach(println)

    spark.stop()
  }
}
