package GraphxTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession


/**
  * function:
  *   graphx的一些相关操作
  *
  * time：2018-2-11
  *
  * @author hongdd
  */
object SomeOprationTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 初始化spark
    val spark = SparkSession.builder()
      .appName("SomeTest")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    // 构造图数据
    //设置顶点和边，注意顶点和边都是用元组定义的Array
    //顶点的数据类型是VD:(String,Int)
    val vertexRDD = sc.parallelize(Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    ))
    //边的数据类型ED:Int
    val edgeRDD = sc.parallelize(Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    ))

    // 构造图  Graph[(String, Int), Int]
    val graph = Graph(vertexRDD, edgeRDD)

    //
    // 图属性相关操作
    //
    println("筛选年龄大于30的节点:")
    graph.vertices.filter{ case (id, (name, age)) => age>30}.foreach(println)

    println("筛选边属性大于5的节点")
    graph.edges.filter(e => e.attr > 4).foreach(println)

    // triplets操作, ((srcId, srcAttr), (dstId, dstAttr), attr)
    println("筛选边属性大于5的triplets")
    graph.triplets.filter(t => t.attr>5).foreach(println)

    println("输出最的入度、出度和度")
    println("最大出度:" + graph.inDegrees.map(_._2).max )
    println("最大入度:" + graph.outDegrees.map(_._2).max)
    println("最大度:" + graph.degrees.map(_._2).max)

    //
    // 转换操作
    //
    println("年龄乘以2")
    graph.mapVertices{ case (id, (name, age)) => (id, (name, age*2))}.vertices.foreach(println)
    println("边属性+10")
    graph.mapEdges{ e => e.attr+10 }.edges.foreach(println)

    //
    // 图分割,结构操作
    //
    println("将年龄大于30的分为子图")
    val subGraph = graph.subgraph(vpred = (_, vd) => vd._2>30)
    println("子图所有节点")
    subGraph.vertices.foreach(println)
    println("子图所有边")
    subGraph.edges.foreach(println)

    //
    // 聚合操作
    //
    println("graphx连接操作")
    val inDegree = graph.inDegrees
    val initGraph = graph.mapVertices((_, vd) => (vd._1, vd._2, 0, 0))
    // outerJoinVertices
    // vd:VD:节点信息内容
    // inDegOut:U -> RDD[(vertexId,U)] 聚合RDD的属性U
    val joinGraph = initGraph.outerJoinVertices(initGraph.inDegrees){
      case (_, vd, inDegOut) => (vd._1, vd._2, inDegOut.getOrElse(0), vd._4)
    }.outerJoinVertices(initGraph.outDegrees){
      case (_, vd, outDegOut) => (vd._1, vd._2, vd._3, outDegOut.getOrElse(0))
    }
    println("输出join后的节点信息")
    joinGraph.vertices.foreach(println)
    println("输出出度和入度相同的节点")
    joinGraph.vertices.filter{ pred => pred._2._3 == pred._2._4 }.foreach(println)

    //
    // 聚合操作
    //
    println("寻找年龄最大的追求者")
//    graph.vertices.foreach(println)
    val oldestFollower = graph.aggregateMessages[(String, Int)](
      sendMsg = {ctx => ctx.sendToDst(ctx.srcAttr._1, ctx.srcAttr._2)},
      mergeMsg = {(a, b) => if(a._2>b._2) a else b}
    )
    oldestFollower.foreach(println)

    // 合并最大追求者
    graph.vertices.leftJoin(oldestFollower){ (_, vd, oldest) =>
      oldest match {
        case None => s"${vd._1} has no flower"
        case Some((name, age)) => s"${name} is the oldest flower of ${vd._1}"
      }
    }.foreach(println)

    //
    // Pregel操作
    //
    println("找出5到各个点的最短距离")
    val sourceID = 5L
    val initalGraph = graph.mapVertices((id, _) =>
      if(id == sourceID) 0.0 else Double.PositiveInfinity)
    /**
      * pregel:
      *   def pregel[A]
             (initialMsg: A,
              maxIter: Int = Int.MaxValue,
              activeDir: EdgeDirection = EdgeDirection.Out)
             (vprog: (VertexId, VD, A) => VD,
              sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
              mergeMsg: (A, A) => A)
           : Graph[VD, ED]
      *  initialMsg : 节点初始化数据，算法开始时每个节点都会受到该数据
      *  maxIter : 算法最大迭代次数，防止算法无法收敛
      *  activeDir : 消息发送方向，默认为出边方向
      *
      *  vprog : 从上一次迭代接受消息 [A],通过算法更新定点的属性
      *  sendMsg : 这个函数接受一个EdgeTriplet参数, 创建发送给边起点或终点的消息
      *  mergeMsg : 函数会对每个顶点接收到的消息进行合并
      */
    val sssp = initalGraph.pregel(Double.PositiveInfinity)(
      (_, dist, newDist) => math.min(dist, newDist),
      triplets => {
        if(triplets.srcAttr + triplets.attr < triplets.dstAttr) {
          Iterator((triplets.dstId, triplets.srcAttr + triplets.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b)
    )

    sssp.vertices.foreach(println)

    spark.stop()
  }
}
