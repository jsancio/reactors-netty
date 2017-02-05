lazy val root = (
  project in file(".")
).settings(
  name := "netty-test",
  version := "0.1.0",
  scalaVersion := "2.11.8",
  libraryDependencies ++= Seq(
    "io.netty" % "netty-codec-http" % "4.1.6.Final",
    "io.netty" % "netty-handler" % "4.1.6.Final",
    "io.monix" %% "monix" % "2.2.1",
    "ch.qos.logback" % "logback-classic" % "1.1.8"
  ),
  scalacOptions ++= Seq(
    "-deprecation"
  )
)
