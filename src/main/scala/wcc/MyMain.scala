package wcc

import ch.qos.logback.classic.{Level, Logger}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.slf4j.LoggerFactory


object MyMain {


  def main(args: Array[String]): Unit = {
    //  Set flink logger level to WARN
    val logger1: Logger = LoggerFactory.getLogger("org.apache.flink").asInstanceOf[Logger]
    logger1.setLevel(Level.WARN)

    // Make logger for this application
    val logger = LoggerFactory.getLogger(getClass.getName)
    logger.info("Starting the Flink job...")
    val PathToJar = "/home/z/IdeaProjects/idwcc-1306/target/scala-2.12/idwcc-flink-new-assembly-0.1.0-SNAPSHOT.jar"
    // Create environment from local host
    val env = ExecutionEnvironment.createRemoteEnvironment(host = "localhost", port = 8081, jarFiles=PathToJar)
//        val env = ExecutionEnvironment.getExecutionEnvironment



    // Path to your CSV file
    val csvPath = "/home/z/IdeaProjects/idwcc-flink-new/data/test_graph.csv"
    logger.warn("graph is loaded!!")

    // Read the CSV file
    val graph = CSVGraph.loadGraphDataset(env=env, graphFilePath = csvPath)

    val newGraph = DistributedWCC.run(graph, env)


//    env.execute()
//    graph.getEdges.map(edge => (edge.f0, edge.f1)).writeAsCsv("/home/z/idwcc-data/undirected.csv", "\n", ",", WriteMode.OVERWRITE) // Assign a short, descriptive name here


    //    logger.warn(s"vertices: ${graph.numberOfVertices}, edges: ${graph.numberOfEdges}")
    // Now you can perform transformations and computations on the `data` DataSet

  }
}

// Define case class
