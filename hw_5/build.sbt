name := "akka-streams-kafka-graph"

version := "1.0.0"

scalaVersion := "2.13.12"

val akkaVersion = "2.6.20"
val akkaKafkaVersion = "2.1.1"

libraryDependencies ++= Seq(
  // Akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  // Akka Kafka (Alpakka)
  "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafkaVersion,

  // Kafka clients
  "org.apache.kafka" % "kafka-clients" % "3.4.0",

  // Logging
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.4.11",

  // Test
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

// Assembly plugin settings для создания fat jar
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// JVM options
fork in run := true
javaOptions in run ++= Seq(
  "-Xms256M",
  "-Xmx1G",
  "-XX:+UseG1GC"
)