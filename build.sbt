import sbt.Keys._

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.13"

lazy val root = (project in file("."))
  .settings(
    name := "idwcc-flink-new",
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % "1.13.6" % Compile,
      "org.apache.flink" %% "flink-streaming-scala" % "1.13.6" % Compile,
      "org.apache.flink" %% "flink-clients" % "1.13.6" % Compile,
      "org.apache.flink" %% "flink-runtime" % "1.13.6" % Compile,
      "org.apache.flink" %% "flink-gelly-scala" % "1.13.6" % Compile,
      "org.apache.flink" %% "flink-gelly" % "1.13.6" % Compile,
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.apache.flink" %% "flink-runtime" % "1.13.6" % Test
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )