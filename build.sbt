name := "hello"

version := "1.0"

scalaVersion := "2.11.9"

// resolvers += "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases"

val jsonDependencies = Seq(
  
  // temp
  // "com.typesafe.play" %% "play-json" % "2.6.7",

  // google api
  "com.google.api-client" % "google-api-client" % "1.28.0",

  // kafka
  "org.apache.kafka" %% "kafka" % "2.1.0",

  // beam
  "org.apache.beam" % "beam-sdks-java-core" % "2.12.0",
  "org.apache.beam" % "beam-sdks-java-extensions-google-cloud-platform-core" % "2.12.0",
  "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % "2.12.0",
  "org.apache.beam" % "beam-sdks-java-io-kafka" % "2.12.0",
  "org.apache.beam" % "beam-runners-direct-java" % "2.12.0",

  // other
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.0.3"
)

libraryDependencies ++=  jsonDependencies

assemblyMergeStrategy in assembly := {
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case x => MergeStrategy.first
}
