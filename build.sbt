import sun.security.util.PathList

name := "LDA"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.5.1"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.1"

libraryDependencies += "org.ansj" % "ansj_seg" % "5.1.1"

libraryDependencies += "org.scalanlp" %% "breeze" % "0.12"

//compile, assemble https://github.com/rjagerman/glint/ first,
// and publish the library jar file to the local ivy2 repository
libraryDependencies += "ch.ethz.inf.da" %% "glint" % "0.1-SNAPSHOT"

libraryDependencies += "redis.clients" % "jedis" % "2.9.0"

libraryDependencies += "io.netty" % "netty-transport-native-epoll" % "4.1.0.CR3"

libraryDependencies  ++= Seq(
  // Native libraries are not included by default. add this if you want them (as of 0.7)
  // Native libraries greatly improve performance, but increase jar sizes.
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
//  "org.scalanlp" %% "breeze-natives" % "0.12",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code.
 // "org.scalanlp" %% "breeze-viz" % "0.12"
)

resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
