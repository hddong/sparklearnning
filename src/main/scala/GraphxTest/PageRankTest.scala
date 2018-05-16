package GraphxTest

import java.util.Calendar

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
  * function:
  *     PageRank 算法
  *     网页排名
  *     结果因子越大 在图中越重要
  * time: 2018-2-11
  * @author hongdd
  */
object PageRankTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("PageRankTest").getOrCreate()

    // 通过边数据集加载图
    val graph = GraphLoader.edgeListFile(spark.sparkContext,
      "./src/main/resources/GraphxData/page-rank-youtube-data.txt")
    val vertexCount = graph.numVertices
    val vertices = graph.vertices
    println(vertexCount + "  " + vertices.count)
    vertices.take(10).foreach(println)

    val edgeCount = graph.numEdges
    val edges = graph.edges
    println(edgeCount + "  " + edges.count)

    val triplets = graph.triplets
    println(triplets.count())
    triplets.take(5).foreach(println)


    val staticPageRank = graph.staticPageRank(10)
    val staticResult = staticPageRank.vertices
    println(staticResult.take(5).mkString("\n"))

    println(Calendar.getInstance().getTime)
    val pageRank = graph.pageRank(0.001).vertices
    println(pageRank.top(5).mkString("\n"))
    println(Calendar.getInstance().getTime)

    spark.stop()

  }
}
