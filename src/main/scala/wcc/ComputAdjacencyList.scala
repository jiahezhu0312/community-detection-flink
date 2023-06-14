package wcc

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation, DataSet}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.graph.pregel.{ComputeFunction, MessageCombiner, MessageIterator}
import org.apache.flink.graph.scala.NeighborsFunctionWithVertexValue
import org.apache.flink.types.{LongValue, NullValue}
import org.apache.flink.util.Collector
import org.apache.flink.graph.{Edge, EdgeDirection, GraphAlgorithm, Vertex}
import org.apache.flink.graph.scala.Graph
import org.slf4j.LoggerFactory
import sun.security.provider.certpath.AdjacencyList
import wcc.CSVGraph.getClass

import collection.JavaConverters._
import scala.collection.immutable.List
import scala.reflect.ClassTag

object ComputAdjacencyList {
  def run[VD: TypeInformation, ED: TypeInformation :ClassTag](graph: Graph[Long, VD, ED], env: ExecutionEnvironment)= {

    val adjListGraph = graph
      .mapVertices((v:Vertex[Long, VD] )=> new VertexCountData(Array.empty[Long], 0))
          .runVertexCentricIteration(
            new AdjListComputeFunction,
            new AdjListCombiner,
            5

          )

//    adjListGraph
    val logger = LoggerFactory.getLogger("ComputAdjacencyList")
    logger.info(s"Computing adjacency List")
//    val result = adjListGraph.getVertices.map(new VertexMapFunction)
//    result.writeAsCsv("/home/z/idwcc-data/AdjListPregel2.csv", "\n", ",", WriteMode.OVERWRITE)
//
//    env.execute()

    adjListGraph

  }
}

//
//class VertexMapFunction extends MapFunction[Vertex[Long, VertexCountData], (Long, Int,  String)] {
//  override def map(x: Vertex[Long, VertexCountData]): (Long, Int, String) = {
//    (x.getId, x.getValue.t, x.getValue.adjList.mkString(","))
//  }
//}



class AdjListComputeFunction[ED: TypeInformation] extends ComputeFunction[Long, VertexCountData, ED, Array[Long]]{
  override def compute(vertex: Vertex[Long, VertexCountData], messageIterator: MessageIterator[Array[Long]]): Unit = {
    if (getSuperstepNumber==1){
      sendMessageToAllNeighbors(Array(vertex.getId))

    }
    else{
      var adjList = Array.empty[Long]
      while (messageIterator.hasNext){
        adjList = adjList ++ messageIterator.next()
      }
      setNewVertexValue(new VertexCountData(adjList, 0))
    }

  }



}

class AdjListCombiner extends MessageCombiner[Long, Array[Long]]{
  override def combineMessages(messageIterator: MessageIterator[Array[Long]]): Unit = {
    var tmp = Array.empty[Long]
    while(messageIterator.hasNext) {
      tmp = tmp ++ messageIterator.next()
    }
    sendCombinedMessage(tmp)
  }
}

