package wcc

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{ ExecutionEnvironment, createTypeInformation}
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.graph.scala.Graph
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

/**
 * This function implements the triangle counting algorithim described in
 * DWCC paper with its first optimization
 * DWCC: https://arxiv.org/abs/1411.0557
 */
object CountTriangle {
  def run[VD: TypeInformation, ED: TypeInformation : ClassTag](graph: Graph[Long, VD, ED], env: ExecutionEnvironment) = {

    // Compute the adjacency list
    val adjDataset = graph.getEdges.map((edge: Edge[Long, ED]) => (edge.getSource, edge.getTarget))
      .groupBy(0)
      .reduceGroup { (in: Iterator[(Long, Long)], out: Collector[(Long, List[Long])]) =>
        val ls = in.toList
        out.collect((ls.head._1, ls.map(_._2)))
      }


    // Launch triangle counting using vertex centric algorithm
    val triangleGraph = graph
      .mapVertices((v: Vertex[Long, VD]) => new VertexCountData(vId = v.getId))
      .joinWithVertices(
        adjDataset,
        (v: VertexCountData, a: List[Long]) => new VertexCountData(vId = v.vId, adjList = a)
      ).runVertexCentricIteration(
      new TriangleComputeFunction,
      new TriangleCountCombiner,
      4
    )

    triangleGraph


  }


  private class VertexCountMessageMap(val messages: Map[Long, (Int, List[Long], Int)]) {
  }

  private class TriangleComputeFunction[ED: TypeInformation] extends ComputeFunction[Long, VertexCountData, ED, VertexCountMessageMap] {

    override def compute(vertex: Vertex[Long, VertexCountData], messageIterator: MessageIterator[VertexCountMessageMap]): Unit = {

      // Every vertex send its degree to its neighbors
      if (getSuperstepNumber == 1) {
        sendMessageToAllNeighbors(new VertexCountMessageMap(Map(vertex.getId -> (vertex.getValue.degree, List.empty[Long], -1))))
      }

      // Check which neighbor has higher or the same degree as itself
      // Send its adjacency list to these vertices
      else if (getSuperstepNumber == 2) {
        while (messageIterator.hasNext) {
          val msg = messageIterator.next().messages
          msg.foreach(pair =>
            if (pair._2._1 >= vertex.getValue.degree) {
              sendMessageTo(pair._1, new VertexCountMessageMap(Map(vertex.getId -> (vertex.getValue.degree, vertex.getValue.adjList, -1))))
            }
          )
        }
      }

        // Each vertex counts the number of triangle it closes with
        // neighboring vertices that have fewer degree by intersecting adjacency lists
        // send the count back to vertices who sent their adjacency lists
        // send a message to itself with the total number of triangle it closes with its lower degree neighbors
      else if (getSuperstepNumber == 3) {
        var vertexTriangleCount = 0
        while (messageIterator.hasNext) {
          val msg = messageIterator.next().messages
          for ((key, value) <- msg) {
            val intersectCounts = vertex.getValue.adjList.intersect(value._2).length
            sendMessageTo(key, new VertexCountMessageMap(Map(vertex.getId -> (vertex.getValue.degree, List.empty[Long], intersectCounts))))
            if (value._1 != vertex.getValue.degree){
              vertexTriangleCount += intersectCounts
            }
          }
        }
        sendMessageTo(vertex.getId, new VertexCountMessageMap(Map( vertex.getId -> (vertex.getValue.degree, List.empty[Long], vertexTriangleCount))))
      }

        // Aggregate the triangle counts it received
        // Divide the value by two due to repeated counts

      else {
        var vertexTriangleCount = 0
        while (messageIterator.hasNext) {
          val msg = messageIterator.next().messages
          msg.foreach(pair => vertexTriangleCount += pair._2._3)
        }

        setNewVertexValue(new VertexCountData(vertex.getId, adjList = vertex.getValue.adjList, t = vertexTriangleCount/2))
      }
    }
  }


  private class TriangleCountCombiner extends MessageCombiner[Long, VertexCountMessageMap] {
    override def combineMessages(messageIterator: MessageIterator[VertexCountMessageMap]): Unit = {
      var newIdAdj = Map.empty[Long, (Int, List[Long], Int)]
      while (messageIterator.hasNext()) {
        newIdAdj ++= messageIterator.next().messages
      }
      sendCombinedMessage(new VertexCountMessageMap(newIdAdj))
    }

  }

}