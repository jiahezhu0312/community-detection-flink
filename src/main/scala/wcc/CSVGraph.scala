package wcc
import scala.collection.mutable
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.graph.{scala, _}
import org.apache.flink.types.NullValue
import org.slf4j.LoggerFactory
import org.apache.flink.graph.scala.Graph
import org.apache.flink.util.Collector



object CSVGraph {

  def loadGraphDataset(env: ExecutionEnvironment, graphFilePath: String) = {
    // Make logger for this application
    val logger = LoggerFactory.getLogger(getClass.getName)
    logger.info("Starting loading graph..")

    // Read a csv with 2 columns representing the edges
    // Using a flatmap to create edge object along both directions

    val edges: DataSet[Edge[Long, NullValue]] = env
      .readCsvFile[(Long, Long)](
        filePath = graphFilePath,
        fieldDelimiter = " "
      )
//      .map(new MapFunction[(Long, Long), Edge[Long, NullValue]] {
//        override def map(value: (Long, Long)): Edge[Long, NullValue] = new Edge(value._1, value._2, NullValue.getInstance())
//      }).name("EdgeMap") // Assign a short, descriptive name here
          .flatMap(new FlatMapFunction[(Long, Long), Edge[Long, NullValue]] {
            override def flatMap(t: (Long, Long), collector: Collector[Edge[Long, NullValue]]): Unit = {
              if (t._1 != t._2) {
                collector.collect(new Edge(t._1, t._2, NullValue.getInstance()))
                collector.collect(new Edge(t._2, t._1, NullValue.getInstance()))
              }
            }

          }).distinct()


    val graph: Graph[Long, NullValue, NullValue] = Graph.fromDataSet(edges, env)
    graph
  }

  def testStream(env: ExecutionEnvironment,
                 graphFilePath: String,
                 bulkToStreamRatio: Float = 0.8f,
                 microBatchCount: Int = 1
                ) = {

    val logger = LoggerFactory.getLogger(getClass.getName)

    val edges: DataSet[Edge[Long, NullValue]] = env
      .readCsvFile[(Long, Long)](
        filePath = graphFilePath,
        fieldDelimiter = " "
      )
      .flatMap(new FlatMapFunction[(Long, Long), Edge[Long, NullValue]] {
        override def flatMap(t: (Long, Long), collector: Collector[Edge[Long, NullValue]]): Unit = {
          if (t._1 != t._2) {
            collector.collect(new Edge(t._1, t._2, NullValue.getInstance()))
            collector.collect(new Edge(t._2, t._1, NullValue.getInstance()))
          }
        }

      }).distinct()

    val maxVertex = edges.map(x => x.getTarget).reduce((a, b) => if (a > b) a else b).collect().head
    val splitVertex = Math.floor(maxVertex * bulkToStreamRatio)

    val bulkEdges = edges.filter( e => (e.getSource < splitVertex )&& (e.getTarget < splitVertex))
    val streamEdges = edges.filter( e => (e.getSource >= splitVertex ) || (e.getTarget >= splitVertex))
    logger.warn(s"splitVertex: ${splitVertex}")
    logger.warn(s"raw bulk edges: ${bulkEdges.count / 2}")
    logger.warn(s"raw stream edges: ${streamEdges.count / 2}")


    val graph: Graph[Long, NullValue, NullValue] = Graph.fromDataSet(bulkEdges, env)

    var (itGraph, cStats_) = IncrementalWCC.prepare(graph, env)
    var cStats: mutable.Map[Long, CommunityData] = mutable.Map(cStats_.toSeq: _*)
    val microBatchSize = Math.floor((maxVertex - splitVertex) / microBatchCount)
    (1 to microBatchCount).foreach(microBatchNum => {
      val lowerLimit = splitVertex + (microBatchNum - 1) * microBatchSize
      val higherLimit = if (microBatchNum == microBatchCount)
        maxVertex + 1
      else
        splitVertex + microBatchNum * microBatchSize
      val microBatchEdges = streamEdges.filter(e => {
        (e.getSource >= lowerLimit || e.getTarget >= lowerLimit) &&
          e.getSource < higherLimit && e.getTarget < higherLimit
      })

      IncrementalWCC.run(itGraph, mutable.Map(cStats.toSeq: _*), microBatchEdges, env) match {
        case (g, s) => itGraph = g; cStats = s
      }
    })
    itGraph.getVertices.map(v => v.getValue.toTuple()).writeAsCsv(s"/home/z/idwcc-defense-data/Incremental.csv", "\n", ",", WriteMode.OVERWRITE)


  }



}
