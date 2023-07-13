package wcc
import org.apache.flink.api.common.functions.{ MapFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.graph.scala.{Graph}
import org.slf4j.LoggerFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scalaz._

/*
 * Performs Community Clustering based on WCC metric
 */
object DistributedWCC {

  import collection.JavaConverters._

  val Logger = LoggerFactory.getLogger(getClass.getName)

  private val threshold = 0.01f

  private var maxRetries = 5
  private var numPartitions = 40
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
    val resultPreprocessing = optimizedGraph.getVertices.map(x=> x.getValue.toTuple())
    val resultPreprocessingGraph = optimizedGraph.getEdges.map(e => (e.getSource, e.getTarget))

    resultPreprocessingGraph.writeAsCsv(s"/home/z/idwcc-defense-data/PreprocessingResultGraph.csv", "\n", ",", WriteMode.OVERWRITE)
    resultPreprocessing.writeAsCsv(s"/home/z/idwcc-defense-data/PreprocessingResult.csv", "\n", ",", WriteMode.OVERWRITE)

    Logger.warn("Phase: Community Initialization Start...")

    val initGraph = performInitialPartition(optimizedGraph, env)
    val resultInitialPartition = initGraph.getVertices.map(x => x.getValue.toTuple())
    resultInitialPartition.writeAsCsv(s"/home/z/idwcc-defense-data/InitialPartitionResult.csv", "\n", ",", WriteMode.OVERWRITE)

    Logger.warn(s"Took ${"%.3f".format((System.currentTimeMillis() - before) / 1000.0)} secconds.")
    Logger.warn("Phase: WCC Iteration Start...")

    val (communityGraph, cStats) = refinePartition(initGraph, vertexCount, env)
    val results = communityGraph.getVertices
    val resultToJoin =  results.map(x => (x.getId, x.getValue))

    val dataGraph = graph.mapVertices((v: Vertex[Long, VD] )=>new VertexData(v.getId, 0, 0))

    val resultGraph = dataGraph.joinWithVertices(
      resultToJoin,
      (v, vd: VertexData) => {
        if (v.cId != vd.cId){
          vd
        } else{
          v
        }
      }
    )

    results.map(v => v.getValue.toTuple()).writeAsCsv(s"/home/z/idwcc-defense-data/FinalPartitionResult.csv", "\n", ",", WriteMode.OVERWRITE)

    (resultGraph, cStats)
  }
  //===========================================================Preprocessing============================================
  /*
    Compute triangle statistics and remove vertices do not close any triangles
   */

  private def preprocess[VD: TypeInformation : ClassTag, ED: TypeInformation : ClassTag](graph: Graph[Long, VD, ED],
                                                                                         env: ExecutionEnvironment,
                                                                                        ) = {
    Logger.warn("Phase: Preprocessing - Graph Optimization")
//    before = System.currentTimeMillis()
    val triangleGraph: Graph[Long, VertexCountData, ED] = CountTriangle.run[VD, ED](graph, env)

    val triangleStats = triangleGraph.getVertices.map(v => (v.getValue.vId, (v.getValue.t, v.getValue.adjList)))

    val resultTriangle = triangleGraph.getVertices.map(x => x.getValue.toTuple())
    resultTriangle.writeAsCsv(s"/home/z/idwcc-defense-data/TriangleCountResult.csv", "\n", ",", WriteMode.OVERWRITE)



    val cleanGraph = triangleGraph.mapEdges((edge: Edge[Long, ED]) => ((0, List.empty[Long]), (0, List.empty[Long])))
      .joinWithEdgesOnSource(
        triangleStats,
        (v, t: (Int, List[Long])) => (t, v._2)
      ).joinWithEdgesOnTarget(
      triangleStats,
      (v, t: (Int, List[Long])) => (v._1, t)
    ).subgraph(
      (v: Vertex[Long, VertexCountData]) => v.getValue.t>0,
      e => e.getValue._1._2.intersect(e.getValue._2._2).length > 0
    )

      val degreesDataset = cleanGraph.getDegrees()

    Logger.warn(s"vertices: ${cleanGraph.getVertices.count}, edges: ${cleanGraph.getEdges.count / 2}")
//    Logger.warn(s"Optimization took: ${System.currentTimeMillis() - before}")
    //          val result =  CountTriangle.run[VD, ED](graph, env).getVertices.map(v => v.getValue.toTuple())
    //        val result = triangleGraph.getVertices.map(new VertexMapFunction)
    //        val result = graph.getEdges.map((e: Edge[Long, ED]) => (e.getSource, e.getTarget))

    //        result.writeAsCsv("/home/z/idwcc-data/TriangleCount.csv", "\n", ",", WriteMode.OVERWRITE)
    //
    //        env.execute()
    cleanGraph.mapVertices(v => new VertexData(vId = v.getId, t = v.getValue.t, vt = 0))
      .joinWithVertices(
        degreesDataset,
        (vData, d: org.apache.flink.types.LongValue) => new VertexData(vId = vData.vId, t = vData.t, vt = d.getValue.toInt/2)
      )
  }



  //===========================================================Initial Partition============================================
  /*
    Vertex centric Initial Partition algorithm based on clustering coefficient
   */
  private def performInitialPartition[ED: TypeInformation](graph: Graph[Long, VertexData, ED], env: ExecutionEnvironment) = {

    val graphPartition = graph.runVertexCentricIteration(
      computeFunction = new InitialPartitionComputeFunction[ED],
      new InitialPartitionCombiner,
      maxIterations = 50
    )
            val result = graphPartition.getVertices.map(v=>v.getValue.toTuple())

        result.writeAsCsv("/home/z/idwcc-data/PFinaledges.csv", "\n", ",", WriteMode.OVERWRITE)

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
      .reduce(_ + _).map(x => x.toDouble / vertexCount).collect()(0)
    var bestCs = computeCommunityStats(graph)
    var bestPartition = graph
    var bestWcc = computeGlobalWCC(bestPartition, env.fromCollection(bestCs.toSeq), vertexCount).collect()(0)

  val result = bestCs.map(v => (v._1, v._2.r,v._2.a, v._2.b)).toSeq
      env.fromCollection(result).writeAsCsv("/home/z/idwcc-data/bestCs.csv", "\n", ",", WriteMode.OVERWRITE)

      env.execute()
    Logger.warn(s"The graph has size ${graph.getVertices.count()}")
      Logger.warn(s"Global WCC ${bestWcc}")
      Logger.warn(s"Global CC ${globalCC}")

      var foundNewBestPartition = true
      var retriesLeft = maxRetries

      do {


        var before = System.currentTimeMillis()
        val movementGraph = getBestMovements(bestPartition,  env.fromCollection(bestCs.toSeq), globalCC, vertexCount)
//  val result = movementGraph.flatMap(new FlatMapFunction[(Long, Map[Long, Int]), (Long, Long, Int)] {
//            override def flatMap(input: (Long, Map[Long, Int]), out: Collector[(Long, Long, Int)]): Unit = {
//              for ((k, v) <- input._2) {
//                out.collect((input._1, k, v))
//              }
//            }
//          })

//          val result = movementGraph.getVertices.map(v=>(v.getId, v.getValue.cId))


//        movementGraph.vertices.count()
        Logger.warn(s"Movement took: ${System.currentTimeMillis() - before}")
        // calculate new global WCC
        before = System.currentTimeMillis()
        val newCs = computeCommunityStats[ED](movementGraph)

        val newWcc = computeGlobalWCC[ED](movementGraph, env.fromCollection(newCs), vertexCount).collect()(0)
        retriesLeft -= 1
        Logger.warn(s"calculate WCC took: ${System.currentTimeMillis() - before}")

        Logger.warn(s"New WCC ${"%.3f".format(newWcc)}")
        Logger.warn(s"Retries left $retriesLeft")

        // if the movements improve WCC apply them
        if (newWcc > bestWcc) {
          if (newWcc / bestWcc - 1 > threshold) {
            Logger.warn("Resetting retries.")
            retriesLeft = maxRetries
          }
//          bestPartition.unpersist(blocking = false)
          bestPartition = movementGraph
          bestWcc = newWcc
          bestCs = newCs
          bestPartition.getVertices.count()
//          movementGraph.unpersist(blocking = false)
        } else {
          foundNewBestPartition = false
        }
      } while (foundNewBestPartition && retriesLeft > 0)


    Logger.warn(s"Best WCC ${"%.3f".format(bestWcc)}")
    Logger.warn(s"sum of all cc has ${globalCC / vertexCount} value")
    Logger.warn(s"sum of all cc has ${globalCC / vertexCount.toLong} value")
  (bestPartition, bestCs)
  }

  private def getBestMovements[ED: TypeInformation](graph: Graph[Long, VertexData, ED],
                                                    bCommunityStats: DataSet[(Long, CommunityData)],
                                                    globalCC: Double, vertexCount: Long) = {
    Logger.warn(s"best movement vertex Count $vertexCount")
    Logger.warn(s"best movement  globalCC $globalCC")

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
    val vertexCommunityDegrees = communityEdges.getEdges
      .map(e => {
        ((e.getSource, e.getValue._2), 1)
      })
      .groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      .map(t => (t._1._1, Map(t._1._2 -> t._2)))
      .groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 ++ t2._2 ))


    val CommunityStatsMap = bCommunityStats.collect().toMap

    graph.joinWithVertices(
      vertexCommunityDegrees,
      (vertex, vcDegrees) => bestMovement(vertex, vcDegrees, mutable.Map[Long, CommunityData]() ++ CommunityStatsMap  , globalCC, vertexCount)
    )
  }


  private def bestMovement(vertex: VertexData, vcDegrees: Map[Long, Int],
                           communityStats: Map[Long, CommunityData],
                           globalCC: Double, vertexCount: Long): VertexData = {
    val newVertex = vertex.copy()


    // WCCR(v,C) computes the improvement of the WCC of a partition when
    // a vertex v is removed from community C and placed in its own isolated community.
    val wccR = computeWccR(vertex, vcDegrees, communityStats(vertex.cId), globalCC, vertexCount)
    Logger.warn(s"for vertex ${vertex.vId}, WccR is $wccR")
    // WCCT (v,C1,C2) computes the improvement of the WCC of a partition when vertex
    // v is transferred from community C1 and to C2.
    var wccT = 0.0d
    var bestC = vertex.cId
    vcDegrees.foreach { case (cId, dIn) =>
      val cData = communityStats(cId)
      if (vertex.cId != cId && cData.r > 1) {
        val dOut = vcDegrees.values.sum - dIn
        val candidateWccT = wccR + WCCMetric.computeWccI(cData, dIn, dOut, globalCC, vertexCount)
        if (candidateWccT > wccT) {
          wccT = candidateWccT
          bestC = cId
        }
      }
    }

    // m ← [REMOVE];
    if (wccR - wccT > 0.00001 && wccR > 0.0d) {
      newVertex.cId = vertex.vId
    }
    // m ← [TRANSFER , bestC];
    else if (wccT > 0.0d) {
      newVertex.cId = bestC
    }
    // m ← [STAY];

    newVertex
  }

  private def computeWccR(vertex: VertexData, cDegrees: Map[Long, Int],
                          cData: CommunityData, globalCC: Double, vertexCount: Long): Double = {
    // if vertex is isolated
    if (cData.r == 1) return 0.0d
    val dIn = cDegrees.getOrElse(vertex.cId, 0)
    val dOut = cDegrees.values.sum - dIn
    val cDataWithVertexRemoved = new CommunityData(
      cData.r - 1,
      cData.a - dIn,
      cData.b + dIn - dOut
    )
    -WCCMetric.computeWccI(cDataWithVertexRemoved, dIn, dOut, globalCC, vertexCount)
  }


  private def computeGlobalWCC[ED: TypeInformation](graph: Graph[Long, VertexData, ED], bCommunityStats: DataSet[(Long, CommunityData)], vertexCount:Long) ={
    val communityNeighborIds = collectCommunityNeighborIds(graph)
    val communityNeighborGraph = graph.mapVertices(v => (v.getValue, Array.empty[Long]))
      .joinWithVertices(
      communityNeighborIds,
      (vData: (VertexData, Array[Long]), neighbors: Array[Long]) =>{
        (vData._1, neighbors)
      }
    )

    val trianglesByCommunity = countCommunityTriangles(communityNeighborGraph)

    val neighborsTriangleGrpah = communityNeighborGraph.mapVertices(v => (v.getValue._1, v.getValue._2, -1))
      .joinWithVertices(
      trianglesByCommunity,
      (vData, ct: Int) => (vData._1, vData._2, ct)
    )

    val res = neighborsTriangleGrpah.getVertices
      .map(v => v.getValue)
      .map(
        new RichMapFunction[(VertexData, Array[Long], Int), Double]{
          var CommunityStats: Traversable[(Long, CommunityData)] = null
          var CommunityStatsMap: Map[Long, CommunityData] = null

          override def open(config: Configuration): Unit = {
            CommunityStats = getRuntimeContext().getBroadcastVariable[(Long, CommunityData)]("CommunityStats").asScala
            CommunityStatsMap = mutable.Map[Long, CommunityData]() ++ CommunityStats.toMap
          }

          def map(v: (VertexData, Array[Long], Int)) = {
            WCCMetric.computeWccV(v._1, CommunityStatsMap(v._1.cId), v._2.length, v._3)
          }
        }
      ).withBroadcastSet((bCommunityStats), "CommunityStats")
      .reduce((a, b) => a + b)
      .map(x => x / vertexCount)
    res
  }

  private def countCommunityTriangles[ED: TypeInformation](graph: Graph[Long, (VertexData, Array[Long]), ED]) ={
    val communityDataset = graph.getVertices
      .map(vertex => (vertex.getId, (vertex.getValue._1.cId, vertex.getValue._2)))

    val communityEdges = graph.mapEdges((e: Edge[Long, ED]) => ((-1L, Array.empty[Long]), (-1L, Array.empty[Long])))
      .joinWithEdgesOnSource(
        communityDataset,
        (e: ((Long, Array[Long]), (Long, Array[Long])), c: (Long, Array[Long])) => (c, (-1L, Array.empty[Long]))
      ).joinWithEdgesOnTarget(
      communityDataset,
      (e: ((Long, Array[Long]), (Long, Array[Long])), c: (Long, Array[Long])) => (e._1, c)
    )

    communityEdges.getEdges
      .map(e => {
        if (e.getValue._1._1 == e.getValue._2._1) {
          val (srcAttr, trgAttr) = e.getValue
          val (smallSet, largeSet) = if (srcAttr._2.length < trgAttr._2.length) {
            (srcAttr._2.toSet, trgAttr._2.toSet)
          }
          else {
            (trgAttr._2.toSet, srcAttr._2.toSet)
          }

          val counter = smallSet.foldLeft(0)((c, vId) => {
            if (vId != e.getSource && vId != e.getTarget && largeSet.contains(vId)) {
              c + 1
            } else {
              c
            }
          }

          )
          (e.getSource, counter)

        } else{
          (e.getSource, 0)

        }

      })
      .groupBy(0)
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
      .map(x => (x._1, x._2 / 2))

  }

  private def collectCommunityNeighborIds[ED: TypeInformation](graph: Graph[Long, VertexData, ED]) ={
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
    val neighborsInSameCommunity = communityEdges.getEdges
      .map(e => {
        if (e.getValue._1 == e.getValue._2) {
          (e.getSource, Array(e.getTarget))
        } else {
          (e.getSource, Array[Long]())
        }
      })
      .groupBy(0)
      .reduce((v1, v2) => (v1._1, v1._2 ++ v2._2))

    neighborsInSameCommunity
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
