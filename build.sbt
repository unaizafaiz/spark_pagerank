
name := "unaiza_faiz_hw5"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.1",
  "com.databricks" %% "spark-xml" % "0.5.0",
  "junit" % "junit" % "4.12" % Test,
  "org.slf4j" % "slf4j-api" % "1.7.12"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}