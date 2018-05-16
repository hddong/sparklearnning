package GraphxTest

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
/**
  * function:
  *     计算图中连通图个数
  * tips:
  *     比较消耗资源,单机跑不成功
  * time: 2018-2-11
  * @author hongdd
  */
object ConnectCompoentsTest {
  def main(args: Array[String]): Unit = {
//    connect-compoents-livejournal-data.txt
    val spark = SparkSession.builder().master("local").appName("ConnectComponents").getOrCreate()

    //通过边数据加载图
    val graph = GraphLoader.edgeListFile(spark.sparkContext,
      "./src/main/resources/GraphxData/connect-components-livejournal-data.txt")

    // 计算连通图
    val connectComponent = graph.connectedComponents().vertices
    connectComponent.take(10).foreach(println)

    spark.stop()
  }
}
