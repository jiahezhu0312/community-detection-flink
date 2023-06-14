package wcc

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.graph._
import org.apache.flink.types.NullValue
import org.slf4j.LoggerFactory
import org.apache.flink.graph.scala.Graph
import org.apache.flink.util.Collector
case object CSVGraph {

  def loadGraphDataset(env: ExecutionEnvironment, graphFilePath: String) = {
    // Make logger for this application
    val logger = LoggerFactory.getLogger(getClass.getName)
    logger.info("Starting loading graph..")
    val edges: DataSet[Edge[Long, NullValue]] = env
      .readCsvFile[(Long, Long)](
        filePath = graphFilePath,
        fieldDelimiter = "\t"
      )
//      .map(new MapFunction[(Long, Long), Edge[Long, NullValue]] {
//        override def map(value: (Long, Long)): Edge[Long, NullValue] = new Edge(value._1, value._2, NullValue.getInstance())
//      }).name("EdgeMap") // Assign a short, descriptive name here
          .flatMap(new FlatMapFunction[(Long, Long), Edge[Long, NullValue]] {
            override def flatMap(t: (Long, Long), collector: Collector[Edge[Long, NullValue]]): Unit = {
              collector.collect(new Edge(t._1, t._2, NullValue.getInstance()))
              collector.collect(new Edge(t._2, t._1, NullValue.getInstance()))
            }

          })


    val graph: Graph[Long, NullValue, NullValue] = Graph.fromDataSet(edges, env)


    graph
  }

}
