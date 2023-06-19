package wcc
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.graph.scala.{Graph, NeighborsFunctionWithVertexValue}
import org.slf4j.LoggerFactory
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.graph.library.TriangleEnumerator
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}
import org.apache.flink.graph.{Edge, EdgeDirection, Vertex}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.reflect.ClassTag

/*
 * Performs Community Clustering based on WCC metric
 */
object DistributedWCC {

  import collection.JavaConverters._

  val Logger = LoggerFactory.getLogger(getClass.getName)

  private val threshold = 0.01f

  private var maxRetries = 5
  private var numPartitions = 200
  var vertexCount = 0L


  def run[VD: TypeInformation : ClassTag, ED: TypeInformation : ClassTag](graph: Graph[Long, VD, ED],
                                                                          env: ExecutionEnvironment,
                                                                          maxRetries: Int = this.maxRetries,
                                                                          partitions: Int = this.numPartitions,
                                                                         ) = {
    this.maxRetries = maxRetries
    this.numPartitions = numPartitions
    this.vertexCount = graph.getVertices.count()


    val before = System.currentTimeMillis()
    Logger.warn("Phase: Preprocessing Start...")
    val optimizedGraph = preprocess(graph, env)
    Logger.warn("Phase: Initialization Start...")

    val initGraph = performInitialPartition[ED](optimizedGraph, env)
    //    println(this.vertexCount)

    val communityMap = refinePartition(initGraph, vertexCount, env)
  }
  //===========================================================Preprocessing============================================
  /*
    Compute triangle statistics and remove vertices do not close any triangles
   */

  private def preprocess[VD: TypeInformation : ClassTag, ED: TypeInformation : ClassTag](graph: Graph[Long, VD, ED],
                                                                                         env: ExecutionEnvironment,
                                                                                        ) = {

    val cleanGraph = CountTriangle.run[VD, ED](graph, env).subgraph(
      v => v.getValue.t > 0,
      new truEdge[ED],
    )
      .mapVertices(v => new VertexData(vId = v.getId, t = v.getValue.t, vt = v.getValue.degree))

    //          val result =  CountTriangle.run[VD, ED](graph, env).getVertices.map(v => v.getValue.toTuple())
    //        val result = triangleGraph.getVertices.map(new VertexMapFunction)
    //        val result = graph.getEdges.map((e: Edge[Long, ED]) => (e.getSource, e.getTarget))

    //        result.writeAsCsv("/home/z/idwcc-data/TriangleCount.csv", "\n", ",", WriteMode.OVERWRITE)
    //
    //        env.execute()
    cleanGraph
  }

  private class truEdge[ED: TypeInformation] extends FilterFunction[Edge[Long, ED]] {
    override def filter(t: Edge[Long, ED]): Boolean = true
  }



  //===========================================================Initial Partition============================================
  /*
    Vertex centric Initial Partition algorithm based on clustering coefficient
   */
  private def performInitialPartition[ED: TypeInformation](graph: Graph[Long, VertexData, ED], env: ExecutionEnvironment) = {

    val graphPartition = graph.runVertexCentricIteration(
      computeFunction = new InitialPartitionComputeFunction[ED],
      new InitialPartitionCombiner,
      maxIterations = 20
    )
    //        val result = graphPartition.getVertices.map(v=>v.getValue.toTuple())
    //
    //    result.writeAsCsv("/home/z/idwcc-data/IPFinal.csv", "\n", ",", WriteMode.OVERWRITE)
    //
    //    env.execute()
    graphPartition


  }



  private class VertexMessageMap(val messages: Map[Long, VertexMessage]) {
  }

  private class InitialPartitionComputeFunction[ED: TypeInformation] extends ComputeFunction[Long, VertexData, ED, VertexMessageMap] {
    override def compute(vertex: Vertex[Long, VertexData], messageIterator: MessageIterator[VertexMessageMap]): Unit = {
      if (getSuperstepNumber == 1) {
        val newData = vertex.getValue.copy()
        newData.changed = true
        setNewVertexValue(newData)
        sendMessageToAllNeighbors(new VertexMessageMap(Map(vertex.getId -> VertexMessage.create(newData))))

      }

      else {
        val newData = vertex.getValue.copy()
        val messages = Map.empty[Long, VertexMessage]
        while (messageIterator.hasNext) {
          messages ++= messageIterator.next().messages
        }

        if (messages.nonEmpty) {
          newData.changed = false
          if (messages.tail.isEmpty && messages.head._2.vId == vertex.getId) {
            // Do nothing
          } else {
            newData.neighbors = if (newData.neighbors.isEmpty) {
              (messages - vertex.getId).values.toList
            } else {
              updateNeighborsCommunities(newData, messages)
            }

            val highestNeighbor = getHighestCenterNeighbor(newData.neighbors)


            if (highestNeighbor.isDefined && VertexMessage.ordering.gt(highestNeighbor.get, VertexMessage.create(vertex.getValue))) {

              newData.changed = newData.isCenter()
              newData.cId = highestNeighbor.get.vId

            } else {
              newData.changed = !newData.isCenter()
              newData.cId = vertex.getId
            }

          }

        }
        else {
          newData.changed = true
        }

        setNewVertexValue(newData)

        // send messages
        val currentVertexMessage = VertexMessage.create(newData)
        for (incomingVertexMessage <- newData.neighbors) {
          if (VertexMessage.ordering.gt(currentVertexMessage, incomingVertexMessage)) {
            if (currentVertexMessage.changed) {
              sendMessageTo(incomingVertexMessage.vId, new VertexMessageMap(Map(currentVertexMessage.vId -> currentVertexMessage)))
              sendMessageTo(newData.vId, new VertexMessageMap(Map(currentVertexMessage.vId -> currentVertexMessage)))
            }
          }
        }


      }

    }


    private def getHighestCenterNeighbor(neighbors: List[VertexMessage]) = {
      neighbors.filter(_.isCenter).sorted(VertexMessage.ordering.reverse).headOption
    }

    private def updateNeighborsCommunities(vertexData: VertexData, neighbors: Map[Long, VertexMessage]) = {
      vertexData.neighbors.map(vData => {
        vData.cId = neighbors.getOrElse(vData.vId, vData).cId
        vData
      })
    }


    private class EdgeToTuple[ED: TypeInformation] extends MapFunction[Edge[Long, ED], (Long, Long)] {
      override def map(t: Edge[Long, ED]): (Long, Long) = (t.getSource, t.getTarget)
    }
  }

  private class InitialPartitionCombiner extends MessageCombiner[Long, VertexMessageMap] {
    override def combineMessages(messageIterator: MessageIterator[VertexMessageMap]): Unit = {
      val combinedMsg = Map.empty[Long, VertexMessage]


      while (messageIterator.hasNext) {
        combinedMsg ++= messageIterator.next().messages

      }
      sendCombinedMessage(new VertexMessageMap(combinedMsg))

    }
  }


  //===========================================================Best Movement============================================
// IN PROGRESS
  private def refinePartition[ED: TypeInformation](graph: Graph[Long, VertexData, ED], vertexCount: Long, env: ExecutionEnvironment) = {
    val globalCC = graph.getVertices.map((v: Vertex[Long, VertexData]) => v.getValue.cc)
      .reduce(_ + _).map(x => x.toDouble / vertexCount)
    var Cs = computeCommunityStats(graph)
    val result = env.fromCollection(Cs.map(pair => (pair._1, pair._2.toTuple())))
    result.writeAsCsv("/home/z/idwcc-data/CommunityStatsNew.csv", "\n", ",", WriteMode.OVERWRITE)

    env.execute()
    //    Logger.warn(s"sum of all cc has ${globalCC / vertexCount} value")
//    Logger.warn(s"sum of all cc has ${globalCC / vertexCount.toLong} value")

  }

  private def computeCommunityStats[ED: TypeInformation](graph: Graph[Long, VertexData, ED]) = {
    val communitySizes = graph.getVertices
      .map(vertex => (vertex.getValue.cId, 1))
      .groupBy(0)
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
      .collect().toMap

    val communityDataset = graph.getVertices
      .map(vertex => (vertex.getId, vertex.getValue.cId))

    val communityEdges = graph.mapEdges((e: Edge[Long, ED]) => (-1L, -1L))
      .joinWithEdgesOnSource(
        communityDataset,
        (e: (Long, Long), c: Long) => (c, -1L)
      ).joinWithEdgesOnTarget(
      communityDataset,
      (e: (Long, Long), c: Long) => (e._1, c)
    )
      .getEdges.flatMap((edge, collector: Collector[((String, Long), Int)]) => {
      if (edge.getValue._1 == edge.getValue._2) {
        collector.collect((("INT", edge.getValue._1), 1))
      } else {
        collector.collect((("EXT", edge.getValue._1), 1))
        collector.collect((("EXT", edge.getValue._2), 1))

      }
    }
    ).groupBy(0)
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
      .map(x => (x._1, x._2 / 2)).collect().toMap


    communitySizes.map({ case (community, size) =>
      val intEdges = communityEdges.getOrElse(("INT", community), 0)
      val extEdges = communityEdges.getOrElse(("EXT", community), 0)
      (community, new CommunityData(size, intEdges, extEdges))
    })
  }



}

//
//}
