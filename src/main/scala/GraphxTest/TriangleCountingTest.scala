package GraphxTest

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
  * function:
  *     计算图中三角形个数
  *     vertices的三角形越多，说明在图中越稳定
  * time: 2018-2-11
  * @author hongdd
  */
object TriangleCountingTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TriangleCountingTest")
      .master("local")
      .getOrCreate()

    val graph = GraphLoader.edgeListFile(spark.sparkContext,
      "./src/main/resources/GraphxData/triangle-count-fb-data.txt")

    graph.vertices.foreach(println)

    val tc = graph.triangleCount()

    tc.vertices.take(5).foreach(println)

    spark.stop()
  }
}
