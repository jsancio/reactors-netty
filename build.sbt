lazy val root = (
  project in file(".")
).settings(
  name := "netty-test",
  version := "0.1.0",
  scalaVersion := "2.12.1",
  libraryDependencies ++= Seq(
    "io.netty" % "netty-codec-http" % "4.1.6.Final",
    "io.netty" % "netty-handler" % "4.1.6.Final"
  )
)
