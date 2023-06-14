package wcc

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation, DataSet}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}

import org.apache.flink.graph.Vertex
import org.apache.flink.graph.scala.Graph

import scala.reflect.ClassTag

object CountTriangle {
  def run[VD: TypeInformation, ED: TypeInformation :ClassTag](graph: Graph[Long, VD, ED], env: ExecutionEnvironment)= {

    val adjListGraph = ComputAdjacencyList.run[VD, ED](graph, env)
      .runVertexCentricIteration(
        new TriangleComputeFunction[ED],
        new TriangleCountCombiner,
        3
      )


    adjListGraph

  }
}

class VertexMapFunction extends MapFunction[Vertex[Long, VertexCountData], (Long, Int, String)] {
  override def map(x: Vertex[Long, VertexCountData]): (Long, Int, String) = {
    (x.getId, x.getValue.t, x.getValue.adjList.mkString(","))
  }
}

class VertexCountMessage(val idAdj: Map[Long, Array[Long]]){
  val degree = idAdj.mapValues(_.length)
}

class TriangleComputeFunction[ED: TypeInformation] extends ComputeFunction[Long, VertexCountData, ED, VertexCountMessage]{

  override def compute(vertex: Vertex[Long, VertexCountData], messageIterator: MessageIterator[VertexCountMessage]): Unit = {

    if(getSuperstepNumber==1){
      sendMessageToAllNeighbors(new VertexCountMessage(Map(vertex.getId -> vertex.getValue.adjList)))
    }
    else if(getSuperstepNumber==2) {
      var tCount = 0
      if (messageIterator.hasNext) {
        while (messageIterator.hasNext) {
          val msg = messageIterator.next()
          for ((id, adjList) <- msg.idAdj) {
            tCount += vertex.getValue.adjList.intersect(adjList).length

          }

        }
        setNewVertexValue(new VertexCountData(vertex.getValue.adjList, tCount/ 2))
      }
    }

//    else if(getSuperstepNumber==2) {
//      while (messageIterator.hasNext){
//        val msg = messageIterator.next()
//          for ((id, adjList) <- msg.idAdj){
//            if (msg.degree(id) >= vertex.getValue.degree) {
//
//              sendMessageTo(id, new VertexCountMessage(Map(id -> vertex.getValue.adjList )))
//
//          }
//        }
//      }
//    }

//    else if(getSuperstepNumber==3){
//      var tCount = 0
//      if (messageIterator.hasNext){
//        while (messageIterator.hasNext) {
//          val msg = messageIterator.next()
//          for ((id, adjList) <- msg.idAdj) {
//            tCount += vertex.getValue.adjList.intersect(adjList).length
//
//          }
//
//        }
//        setNewVertexValue(new VertexCountData(vertex.getValue.adjList, tCount))
//      }
//
//
//    }
  }
}

class TriangleCountCombiner extends MessageCombiner[Long, VertexCountMessage]{
  override def combineMessages(messageIterator: MessageIterator[VertexCountMessage]): Unit = {
    var newIdAdj = Map.empty[Long, Array[Long]]
    while (messageIterator.hasNext()){
      newIdAdj ++= messageIterator.next().idAdj
    }
    sendCombinedMessage(new VertexCountMessage(newIdAdj))
  }

}