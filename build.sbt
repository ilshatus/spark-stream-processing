name := "spark-recommendation"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.3.2"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/",
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
)
//addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.github.catalystcode" %% "streaming-rss-html" % "1.0.2" ,
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}

