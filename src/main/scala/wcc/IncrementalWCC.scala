package wcc

import ch.qos.logback.classic.Logger
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.graph.scala.Graph
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{Level, Logger}
import org.apache.flink.graph.pregel.ComputeFunction
import org.apache.flink.graph.{Edge, Triplet, Vertex}
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.reflect.ClassTag

object IncrementalWCC {
  val logger = LoggerFactory.getLogger(IncrementalWCC.getClass)
  private var numPartitions = 200
  private var globalCC = 0.0
  private var vertexCount = 0L

  def run[ED: TypeInformation : ClassTag](itGraph: Graph[Long, VertexData, ED], itCStats: Map[Long, CommunityData],
                               newEs: DataSet[Edge[Long, ED]], env: ExecutionEnvironment) = {
    val before = System.currentTimeMillis()
    logger.warn(s" raw triangle${itGraph.getVertices.map(v=>(v.getId, v.getValue.t)).collect()} ")

    logger.warn(s"Dataset has ${newEs.count / 2} incoming edges")
    logger.warn("Phase: Merging starts...")
    val (fullGraph, mergeTime, newVertices, borderVertices) = merge(itGraph, itCStats, newEs, env)
//    logger.warn(s"incremental took ${"%.3f".format((System.currentTimeMillis() - before - mergeTime) / 1000.0)} seconds")
    logger.warn("Phase: Initial Partition starts...")
    val before2 = System.currentTimeMillis()
    logger.warn(s" before initial partition triangle${fullGraph.getVertices.map(v=>(v.getId, v.getValue.t)).collect()} ")

    val initGraph = performInitialPartition(fullGraph, newVertices++ Map[Long, VertexData](), borderVertices ++ Map[Long, VertexData]())
//    initGraph.getVertices.count()
    logger.warn(s"Initial Partition Vertex Data took: ${System.currentTimeMillis() - before2}")
    logger.warn(s" initial partition${initGraph.getVertices.map(v=>v.getValue.cId).collect()} ")

    logger.warn("Phase: Partition Improvement starts...")
    val (finalGraph, finalCommunityStats) = refinePartition(initGraph, newVertices, borderVertices, env)

    logger.warn(s"incremental took ${"%.3f".format((System.currentTimeMillis() - before - mergeTime) / 1000.0)} seconds")

    val bestWcc = computeGlobalWcc(finalGraph, env.fromCollection(finalCommunityStats.toSeq), vertexCount)
    logger.warn(s"Best WCC ${"%.3f".format(bestWcc.collect().head)}")

    val results = finalGraph.getVertices
    val resultToJoin = results.map(x => (x.getId, x.getValue))

    val dataGraph = itGraph.mapVertices((v: Vertex[Long, VertexData]) => new VertexData(v.getId, 0, 0))

    val resultGraph = dataGraph.joinWithVertices(
      resultToJoin,
      (v, vd: VertexData) => {
        if (v.cId != vd.cId) {
          vd
        } else {
          v
        }
      }
    )

    (resultGraph, itCStats)

  }

  def prepare[VD: TypeInformation : ClassTag, ED: TypeInformation : ClassTag](graph: Graph[Long, VD, ED], env: ExecutionEnvironment) = {

    val communityGraph = DistributedWCC.run(graph, env)
    communityGraph
  }

  def merge[ED: TypeInformation : ClassTag, VD: TypeInformation: ClassTag](itGraph: Graph[Long, VertexData, ED], itCStats: Map[Long, CommunityData],
                                                      newEs: DataSet[Edge[Long, ED]], env: ExecutionEnvironment) = {

    var before = System.currentTimeMillis()
    val batchVertices = Graph.fromDataSet(newEs, env).getVertices.map(v => (new VertexData(v.getId, 0, 0)))
    logger.warn(s"batchVertices  new graph has ${batchVertices.count()} vertices")

    val bVertices = batchVertices.map(vd => (vd.vId, vd)).collect().toMap
    val borderVertices = mutable.Map(itGraph.getVertices.filter(v => bVertices.contains(v.getId)).map(v => (v.getId, v.getValue)).collect().toMap.toSeq: _*)
    val newVs = batchVertices.filter(v => !borderVertices.contains(v.vId) )
    val newVertices = mutable.Map(newVs.map(v => (v.vId, v)).collect().toMap.toSeq: _*)


    logger.warn(s"New graph has ${borderVertices.size} border vertices " + s"and ${newVs.count} new vertices")

    val fg = Graph.fromDataSet(itGraph.getVertices.union(newVs.map(v => new Vertex(v.vId, v))), itGraph.getEdges.union(newEs), env)

    logger.warn(s"Full graph has ${fg.getVertices.count} vertices")
    logger.warn(s"Full graph should have ${newVs.count} vertices")
    logger.warn(s"Full graph should have ${newEs.count} edges")

    fg.getVertices.count;
    fg.getEdges.count
    val mergeTime = System.currentTimeMillis() - before

    before = System.currentTimeMillis()
    val fullGraph = updateVertexData(fg, borderVertices, newVertices)
    vertexCount = fullGraph.getVertices.count()
    logger.warn(s"Updating Vertex Data took: ${System.currentTimeMillis() - before}")
    logger.warn(s"graph vertices has cid ${fullGraph.getVertices.map { v => v.getValue.cId }.collect()}")
    globalCC = fullGraph.getVertices.map { v => v.getValue.cc }.reduce((x, y) => x+y).collect().head / vertexCount
    logger.warn(s"Global CC $globalCC")
    (fullGraph, mergeTime, newVertices, borderVertices)
  }

  def updateVertexData[ED: TypeInformation](graph: Graph[Long, VertexData, ED],
                                            borderVertices: Map[Long,  VertexData],
                                            newVertices: Map[Long, VertexData]
                                           )={
    logger.warn(s" graph has cid in Update Vertex${graph.getVertices.map(v=>v.getValue.cId).collect()} ")

    val newGraphNeighbors = graph.getTriplets().map(
      (t: Triplet[Long, VertexData, ED]) => {
        if ((borderVertices.contains(t.getSrcVertex.getId)) || (newVertices.contains(t.getSrcVertex.getId)) ) (t.getSrcVertex.getId, Array(t.getTrgVertex.getId)) else{
          (t.getSrcVertex.getId, Array[Long]())
        }

      }
    ).groupBy(0).reduce((x1, x2) =>(x1._1, x1._2 ++ x2._2))
    val neighborGraph = graph.mapVertices(v=> Array[Long]())
      .joinWithVertices(newGraphNeighbors,
        (vData, newNeighbors: Array[Long]) => if(newNeighbors.size>0) newNeighbors else vData)

    val borderStats = neighborGraph.getTriplets().map((t: Triplet[Long, Array[Long], ED]) => {
      val borderEdge = borderVertices.contains(t.getSrcVertex.getId) && borderVertices.contains(t.getTrgVertex.getId)
      val newEdge = newVertices.contains(t.getSrcVertex.getId) || newVertices.contains(t.getTrgVertex.getId)
      if (newEdge || borderEdge) {
        val (smallSet: Set[Long], largeSet: Set[Long]) = if (t.getSrcVertex.getValue.length < t.getTrgVertex.getValue.length) {
          (t.getSrcVertex.getValue.toSet, t.getTrgVertex.getValue.toSet)
        } else {
          (t.getTrgVertex.getValue.toSet, t.getSrcVertex.getValue.toSet)
        }
        var newVt = true
        val counter = smallSet.foldLeft(0)((c, vId) => {
          if (vId != t.getSrcVertex.getId && vId != t.getTrgVertex.getId && largeSet.contains(vId)) {
            if (newEdge || newVertices.contains(vId)) {
              c + 1
            } else {
              newVt = false
              c
            }
          } else {
            c
          }
        })
        val i = if (counter > 0 && newVt) 1 else 0
        (t.getSrcVertex.getId, (counter, i))

      } else  (t.getSrcVertex.getId, (0, 0))

    }
    ).groupBy(0).reduce((x1, x2) => (x1._1, (x1._2._1 + x2._2._1, x1._2._2+ x2._2._2)))
    logger.warn(s" graph has borderStats in Update Vertex result ${borderStats.collect()} ")

    val result = graph.joinWithVertices(
      borderStats,
      (vData, statsOpt: Tuple2[Int, Int]) => (
        if (statsOpt!= (0, 0)) {
          val (t, vt) = if (borderVertices.contains(vData.vId)){
            (vData.t + statsOpt._1 / 2, vData.vt + statsOpt._2)
          } else {
            (statsOpt._1 / 2, statsOpt._2)
          }
          new VertexData(vData.vId, t, vt)

        } else {
          vData.copy()
        }
        )
    )
    logger.warn(s" graph has cid in Update Vertex result ${result.getVertices.map(v=>v.getValue.cId).collect()} ")
    result
  }

  // ===========================================Initial PArtition=====================================================

  def performInitialPartition[ED: TypeInformation](graph: Graph[Long, VertexData, ED],
                                            newVertices: Map[Long, VertexData],
                                            borderVertices: Map[Long,  VertexData]
                                           ) ={
    val neighborId = graph.getEdges.map( (e:  Edge[Long, ED])=>
      (e.getSource, Array(e.getTarget))
    ).groupBy(0)
      .reduce(
        (x1, x2) => (x1._1, x1._2 ++ x2._2)
      )

    val initGraphEmpty = graph.mapVertices(v => (v.getValue, Array[Long]()))
    val initGraphFull = initGraphEmpty.joinWithVertices(
      neighborId,
      (vData, neighbors:Array[Long]) => if (neighbors != null) (vData._1, neighbors) else{vData}
    )
    val edgeBoolean = initGraphFull.getTriplets().map((t:  Triplet[Long, (VertexData, Array[Long]), ED])=>(t.getSrcVertex.getId, t.getTrgVertex.getId, t.getSrcVertex.getValue._2.intersect(t.getTrgVertex.getValue._2).nonEmpty))
    val initGraphEdge = initGraphFull.mapEdges((e: Edge[Long, ED])=>false).joinWithEdges(
      edgeBoolean,
      (ev, b: Boolean) => b
    )
    val initGraph =initGraphEdge
    .subgraph(
    vertexFilterFun = {v => v.getValue._1.t > 0},
    edgeFilterFun = {e =>e.getValue })
      .mapVertices(v=>(v.getValue._1))

    logger.warn(s"Before pregel in initial partition triangle ${initGraphEdge.getVertices.map(v=>(v.getId, v.getValue._1.t)).collect()}")
    logger.warn(s"Before pregel in initial partition edge ${initGraphEdge.getEdges.map(v=>(v.getSource, v.getTarget,v.getValue)).collect()}")

    val pregelGraph = initGraph.runVertexCentricIteration(
      computeFunction = new InitialPartitionComputeFunction,
      new InitialPartitionCombiner,
      maxIterations = 50
    )

    pregelGraph.mapVertices(v => {
      val data = v.getValue
      data.changed = true
      data.neighbors = List.empty
      data
    })
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

// ==============================================Partition Refinement=================================================
  def refinePartition(graph: Graph[Long, VertexData, Boolean],
                      newVertices: mutable.Map[Long, VertexData],
                      borderVertices: mutable.Map[Long, VertexData],
                      env: ExecutionEnvironment) ={
    val gcc = this.globalCC
    val vc = this.vertexCount

    var bestCs = computeCommunityStats(graph)
    var movementGraph = graph
    var it = 0
    do {
      it += 1
      logger.warn(s"Current partition1 ${movementGraph.getVertices.map(v=>(v.getValue.vId, v.getValue.cId)).collect()}")

      val cvDegreesMap = movementGraph.getTriplets().map(t => (t.getSrcVertex.getId, Seq((t.getTrgVertex.getValue.cId, 1))))
      .groupBy(0)
      .reduceGroup { (in:  Iterator[(Long, Seq[(Long, Int)])], out: Collector[(Long, Seq[(Long, Int)])]) =>
        val tuples = in.toSeq
        val combinedSeq = tuples.flatMap(_._2)
        out.collect((tuples.head._1, combinedSeq))
      }

      val cvDegrees = cvDegreesMap.flatMap(new FlatMapFunction[(Long, Seq[(Long, Int)]), (Long, Map[Long, Int])] {
        override def flatMap(input: (Long, Seq[(Long, Int)]), out: Collector[ (Long, Map[Long, Int])]): Unit = {
          val (vId, seq) = input
          seq.groupBy(_._1).foreach { case (cId, tuples) =>
            out.collect((vId, Map(cId-> tuples.map(_._2).sum)))
          }
        }
      })

      logger.warn(s"cvDegrees ${cvDegrees.collect()}")



        val bestGraph = movementGraph.joinWithVertices(
        cvDegrees,
          (vertex, cDegrees:  Map[Long, Int]) =>{
            val newVertex = vertex.copy()
            newVertex.changed = false
            val candidates = mutable.Map(cDegrees.keys.collect{ case cId if bestCs.contains(cId) => (cId, bestCs(cId))}.toSeq: _*)
            bestMovement(newVertex, cDegrees, bestCs(vertex.cId), candidates, gcc, vc)

          }
        )

      movementGraph = bestGraph
      logger.warn(s"Current partition 2${movementGraph.getVertices.map(v=>(v.getValue.vId, v.getValue.cId)).collect()}")
      bestCs = computeCommunityStats(movementGraph)

    } while(it < 5)

    val finalGraph = movementGraph.mapVertices(v => {
      val data = v.getValue.copy()
      data.changed = false
      data
    })

    finalGraph.getVertices.count()

    (finalGraph, bestCs)
  }

  private def bestMovement(vertex: VertexData, cDegrees: Map[Long, Int],
                           community: CommunityData,
                           candidates: Map[Long, CommunityData],
                           globalCC: Double, vertexCount: Long): VertexData = {

    val wccR = computeWccR(vertex, cDegrees, community, globalCC, vertexCount)
    var wccT = 0.0d
    var bestC = vertex.cId

    candidates.foreach { case (cId, cData) =>
      if (vertex.cId != cId && cData.r > 1) {
        val dIn = cDegrees(cId)
        val dOut = cDegrees.values.sum - dIn
        val candidateWccT = wccR + WCCMetric.computeWccI(cData, dIn, dOut, globalCC, vertexCount)
        if (candidateWccT > wccT) {
          wccT = candidateWccT
          bestC = cId
        }
      }
    }

    // m ← [REMOVE];
    if (wccR - wccT > 0.00001 && wccR > 0.0d) {
      vertex.cId = vertex.vId
    }
    // m ← [TRANSFER , bestC];
    else if (wccT > 0.0d) {
      vertex.cId = bestC
    }
    // m ← [STAY];

    vertex
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

  private def computeCommunityStats[ED: TypeInformation](graph: Graph[Long, VertexData, ED]) = {
    val communitySizes = graph.getVertices
      .map(vertex => (vertex.getValue.cId, 1))
      .groupBy(0)
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
      .collect().toMap
    logger.warn(s"Computing community stats${
      graph.getVertices
        .map(vertex => (vertex.getValue.cId, 1)).collect()}")

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

  private def computeGlobalWcc[ED: TypeInformation](graph: Graph[Long, VertexData, ED], bCommunityStats: DataSet[(Long, CommunityData)], vertexCount: Long) = {
    val communityNeighborIds = collectCommunityNeighborIds(graph)
    val communityNeighborGraph = graph.mapVertices(v => (v.getValue, Array.empty[Long]))
      .joinWithVertices(
        communityNeighborIds,
        (vData: (VertexData, Array[Long]), neighbors: Array[Long]) => {
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
        new RichMapFunction[(VertexData, Array[Long], Int), Double] {
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

  private def countCommunityTriangles[ED: TypeInformation](graph: Graph[Long, (VertexData, Array[Long]), ED]) = {
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

        } else {
          (e.getSource, 0)

        }

      })
      .groupBy(0)
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
      .map(x => (x._1, x._2 / 2))

  }

  private def collectCommunityNeighborIds[ED: TypeInformation](graph: Graph[Long, VertexData, ED]) = {
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

}

