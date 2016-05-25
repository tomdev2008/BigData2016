name := "spark1hw4"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "com.restfb" % "restfb" % "1.23.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.1"
//libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "1.6.1"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.3.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.1"
//libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop_2.10" % "5.0.0-alpha2"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M2"

resolvers += "clojars" at "https://clojars.org/repo"
resolvers += "conjars" at "http://conjars.org/repo"

resolvers += "sonatype-oss" at "http://oss.sonatype.org/content/repositories/snapshots"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

/*
// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.apache.spark" %% "spark-streaming" % "1.1.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.1.0",
  "org.elasticsearch" % "elasticsearch-hadoop" % "2.1.0"
)

resolvers += "conjars.org" at "http://conjars.org/repo"*/
