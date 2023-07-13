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
    logger.warn("Starting the Flink job...")
    val PathToJar = "/home/z/IdeaProjects/idwcc-1306/target/scala-2.12/idwcc-flink-new-assembly-0.1.0-SNAPSHOT.jar"
    logger.warn("Getting context!!")

    // Create environment from local host
    val env = ExecutionEnvironment.createRemoteEnvironment(host = "localhost", port = 8081, jarFiles=PathToJar)

    logger.warn("We have context!!")


    // Path to your CSV file

    // val csvPath = "/home/z/IdeaProjects/idwcc-flink-new/data/test_graph.csv" //SNAP email
    val csvPath = "/home/z/idwcc-defense-data/source_graph.csv" //small Graph

    val idwcc = false

    if (idwcc){
      val graph = CSVGraph.testStream(env=env, graphFilePath = csvPath)

    } else{
//       Read the CSV file
      val graph = CSVGraph.loadGraphDataset(env=env, graphFilePath = csvPath)
      logger.warn("graph is loaded!!")
      logger.warn(s"vertices: ${graph.getVertices.count}, edges: ${graph.getEdges.count / 2}")
      val newGraph = DistributedWCC.run(graph, env)
    }




    env.execute()


  }


}

