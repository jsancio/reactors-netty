lazy val root = (project in file("."))
  .settings(
    name := "monix-netty",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.12.3",
    libraryDependencies ++= Seq(
      "io.netty" % "netty-codec-http" % "4.1.14.Final",
      "io.netty" % "netty-handler" % "4.1.14.Final",
      "io.monix" %% "monix" % "2.3.0",
      "ch.qos.logback" % "logback-classic" % "1.2.3"
    ),
    scalacOptions ++= Seq(
      "-Xfatal-warnings",
      "-Ywarn-unused-import",
      "-deprecation"
    )
  )
