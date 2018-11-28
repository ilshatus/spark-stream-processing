name := "Stream processing"


scalaVersion := "2.11.7"
val sparkVersion = "2.1.0"


resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.github.catalystcode" %% "streaming-rss-html" % "1.0.2" ,
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
mainClass in (Compile, run) := Some("StreamProcessor")
mainClass in (Compile, packageBin) := Some("SteamProcessor")