ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "data-spark-streaming"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.0",
  "com.amazonaws" % "aws-java-sdk-redshift" % "1.12.102",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.2.0",
  "com.databricks" % "spark-redshift_2.11" % "2.0.1",
  "com.amazon.redshift" % "redshift-jdbc42" % "2.0.0.7",
  "com.github.mrpowers" %% "spark-daria" % "1.2.3"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

mainClass in assembly := Some("com.example.KafkaToRedshift")

assemblyJarName in assembly := "kafka-to-redshift.jar"

javaOptions in run ++= Seq(
  "-Dlog4j.configuration=file:./log4j.properties",
  "-Dcom.amazonaws.sdk.disableCertChecking"
)