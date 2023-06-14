package wcc
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.graph.scala.{Graph, NeighborsFunctionWithVertexValue}
import org.slf4j.LoggerFactory
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.graph.{Edge, EdgeDirection, Vertex}
import org.apache.flink.util.Collector

import scala.reflect.ClassTag
object DistributedWCC {
  import collection.JavaConverters._

  val Logger = LoggerFactory.getLogger(getClass.getName)
  Logger.info("Starting loading graph..")

  private val threshold = 0.01f

  private var maxRetries = 5
  private var numPartitions = 200
  var vertexCount = 0L

  def run[ VD:TypeInformation: ClassTag, ED: TypeInformation: ClassTag](graph: Graph[Long, VD, ED],
                                                                        env: ExecutionEnvironment,
                                                                        maxRetries: Int = this.maxRetries,
                                                                        partitions: Int = this.numPartitions,
                                                                        isCanonical: Boolean = false
                                                                       ) ={
    this.maxRetries = maxRetries
    this.numPartitions = numPartitions
    this.vertexCount = graph.getVertices.count()
    val before = System.currentTimeMillis()
    Logger.warn("Phase: Preprocessing Start...")
    val optimizedGraph = preprocess(graph, env, isCanonical)
    val initGraph = performInitialPartition(optimizedGraph)
    val initCommunityMap = initGraph.vertices.mapValues((vId, vData) => vData.cId)
    //    println(this.vertexCount)
  }


  def preprocess[VD: TypeInformation : ClassTag, ED: TypeInformation : ClassTag](graph: Graph[Long, VD, ED],
                                                                                 env: ExecutionEnvironment,
                                                                                 isCanonical: Boolean) = {
    Logger.warn("Phase: Preprocessing - Counting Triangles")
    var before = System.currentTimeMillis()

    val triangleGraph = CountTriangle.run[VD, ED](graph, env)
    val cleanGraph = triangleGraph.subgraph(
      v => v.getValue.t>0,
      new truEdge[ED],
    )
      .mapVertices(v => new VertexData(v.getId, t = v.getValue.t, vt = v.getValue.degree))
    Logger.warn(s"Counting Triangles took: ${System.currentTimeMillis() - before}")

    cleanGraph

//    val result = cleanGraph.getVertices.map(new VertexMapFunction)
//    val result = cleanGraph.getEdges.map(new EdgeToTuple[ED]).map(pair => (Math.min(pair._1, pair._2), Math.max(pair._1, pair._2))).distinct
//    result.writeAsCsv("/home/z/idwcc-data/CleanGraphEdge.csv", "\n", ",", WriteMode.OVERWRITE)
//
//    env.execute()
    //    adjListGraph.getVertices.print()

//    val tcGraph = countTriangle(graph, tList.asInstanceOf[DataSet[Tuple3[LongValue, LongValue, LongValue]]])
  }

  def performInitialPartition[ED:TypeInformation](value: Graph[Long, VertexData, ED])={

  }

  private class truEdge[ED:TypeInformation] extends FilterFunction[Edge[Long, ED]]{
    override def filter(t: Edge[Long, ED]): Boolean = true
  }

  private class EdgeToTuple[ED: TypeInformation] extends MapFunction[Edge[Long, ED], (Long, Long)] {
    override def map(t: Edge[Long, ED]): (Long, Long) = (t.getSource, t.getTarget)
  }
}
