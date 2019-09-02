ThisBuild / scalaVersion := "2.12.7"
ThisBuild / organization := "com.mandelag"

lazy val mba = (project in file("."))
  .settings(
    name := "market-basket-analysis",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.5.25",
      "com.typesafe.akka" %% "akka-stream" % "2.5.25",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
)
