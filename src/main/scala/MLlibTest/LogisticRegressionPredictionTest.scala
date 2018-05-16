package MLlibTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.asc

/**
  * function:
  *     逻辑回归预测算法
  *     预测模型
  * time: 2018-2-11
  * @author hongdd
  */
object LogisticRegressionPredictionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LogisticRegressionPredictionTest")
      .master("local")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // 加载数据
    val data = spark.read
      .option("header","false")
      .option("inferSchema","true")
      .csv("./src/main/resources/MLlibData/tmp_matrix_input_CmHD8VhU4GY8QvD.log")
      .toDF("date_str", "grid_num", "crime_num", "crime_bi", "same_crime", "bank",
        "period0_temporal_spatial_0_0", "period0_temporal_spatial_0_1",
        "period0_temporal_spatial_1_0", "period0_temporal_spatial_1_1",
        "period0_temporal_spatial_2_0", "period0_temporal_spatial_2_1",
        "period0_temporal_spatial_3_0", "period0_temporal_spatial_3_1",
        "period0_temporal_spatial_4_0", "period0_temporal_spatial_4_1")
    val colArray = Array("date_str", "grid_num", "crime_num", "crime_bi", "same_crime", "bank",
      "period0_temporal_spatial_0_0", "period0_temporal_spatial_0_1",
      "period0_temporal_spatial_1_0", "period0_temporal_spatial_1_1",
      "period0_temporal_spatial_2_0", "period0_temporal_spatial_2_1",
      "period0_temporal_spatial_3_0", "period0_temporal_spatial_3_1",
      "period0_temporal_spatial_4_0", "period0_temporal_spatial_4_1")
    val colArray2 = Array("same_crime", "bank",
      "period0_temporal_spatial_0_0", "period0_temporal_spatial_0_1",
      "period0_temporal_spatial_1_0", "period0_temporal_spatial_1_1",
      "period0_temporal_spatial_2_0", "period0_temporal_spatial_2_1",
      "period0_temporal_spatial_3_0", "period0_temporal_spatial_3_1",
      "period0_temporal_spatial_4_0", "period0_temporal_spatial_4_1")
    val vecDF = new VectorAssembler().setInputCols(colArray2).setOutputCol("selectFeatures").transform(data)

    val lrModel = LogisticRegressionModel.load("./src/main/resources/MLlibData/model/logisticMode1")
    val result = lrModel.transform(vecDF)
      .select("date_str", "grid_num", "crime_num", "probability", "prediction").cache()

    val data2 = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .csv("./src/main/resources/MLlibData/tmp_matrix_output_CmHD8VhU4GY8QvD.log")
      .toDF("date_str", "grid_num", "crime_num_r", "probability_r")
    result.join(data2,Seq("date_str", "grid_num")).orderBy(asc("date_str"), asc("probability")).show(false)

    result.orderBy(asc("date_str"), asc("probability")).rdd.map{row =>
      val pre = row.get(3).toString.split(",")(1).replace("]", "")
      Seq(row.get(0), row.get(1), row.get(4), pre).mkString(",")
    }.repartition(1).saveAsTextFile("./src/main/resources/MLlibData/result")

    spark.stop()
  }
}
