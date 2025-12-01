import sbtassembly.AssemblyPlugin
import sbtassembly.AssemblyPlugin.autoImport._

ThisBuild / organization := "flightpipeline"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"

lazy val sparkVer = "3.5.1"   // OK avec Spark 3.5.2 installé côté runtime

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"    % sparkVer % "provided",
  "org.apache.spark" %% "spark-mllib"  % sparkVer % "provided",
  "com.typesafe"      % "config"       % "1.4.3",

  // Delta Lake pour Spark 3.5.x  -> utiliser delta-spark (et pas delta-core)
  "io.delta"         %% "delta-spark"  % "3.2.0"  % "provided",

  // (facultatif) API logging — l’implémentation est fournie par le runtime Spark
  "org.slf4j"         % "slf4j-api"            % "1.7.36" % "provided",
  "org.apache.logging.log4j" % "log4j-api"     % "2.23.1" % "provided",
  "org.apache.logging.log4j" % "log4j-core"    % "2.23.1" % "provided",
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.23.1" % "provided"
)

Compile / scalacOptions ++= Seq("-deprecation","-feature","-encoding","utf8")

// Activer sbt-assembly pour produire un fat-jar
enablePlugins(AssemblyPlugin)

assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case _                            => MergeStrategy.first
}