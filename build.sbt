ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "net.eazel"

lazy val auctionHistory = (project in file("auction-history"))
  .settings(
    name := "auction-history-data-spark-streaming",
    assembly / assemblyJarName := s"${name.value}.jar",
    libraryDependencies ++= commonDependencies ++ auctionHistoryDependencies
  )

lazy val artfactsSpotlight = (project in file("artfacts"))
  .settings(
    name := "artfacts-spotlight-data-spark-streaming",
    assembly / assemblyJarName := s"${name.value}.jar",
    libraryDependencies ++= commonDependencies ++ artfactsSpotlightDependencies
  )

lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.0",
  "com.databricks" % "spark-redshift_2.11" % "2.0.1" % "provided",
  "com.amazonaws" % "aws-java-sdk-redshift" % "1.12.102" % "provided",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "3.2.0" % "provided",
  "com.amazon.redshift" % "redshift-jdbc42" % "2.0.0.7" % "provided",
)

lazy val auctionHistoryDependencies = Seq(
  "com.github.mrpowers" %% "spark-daria" % "1.2.3",
  "edu.stanford.nlp" % "stanford-corenlp" % "4.3.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "4.3.1" classifier "models",
  "io.github.spark-redshift-community" %% "spark-redshift" % "4.2.0",
  "net.java.dev.jets3t" % "jets3t" % "0.9.4"
)

lazy val artfactsSpotlightDependencies = Seq(
  "com.desmondyeung.hashing" %% "scala-hashing" % "0.1.0"
)

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// ThisBuild / assemblyJarName := "data-spark-streaming.jar"
// ThisBuild / assemblyOption := (ThisBuild / assemblyOption).value.copy(includeScala = false)

// assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// mainClass in assembly := Some("com.example.KafkaToRedshift")

// assemblyJarName in assembly := "kafka-to-redshift.jar"

run / javaOptions ++= Seq(
  "-Dlog4j.configuration=file:./log4j.properties",
  "-Dcom.amazonaws.sdk.disableCertChecking"
)