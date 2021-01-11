name := "GeoIp"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
"com.databricks" %% "spark-csv" % "1.5.0",
"org.apache.spark" %% "spark-yarn" % "2.4.7" % "provided",
"org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
"org.apache.spark" %% "spark-sql" % "2.4.7" % "provided"
)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
