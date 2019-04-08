ThisBuild / name := "sstdemo"
ThisBuild / scalaVersion := "2.12.7"
ThisBuild / organization := "com.gaufoo"

lazy val root = (project in file("."))
    .settings(
      libraryDependencies += "org.slf4s" %% "slf4s-api" % "1.7.25",
      libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
      libraryDependencies += "org.mpierce.metrics.reservoir" % "hdrhistogram-metrics-reservoir" % "1.1.0",
      libraryDependencies += "org.scalatest" % "scalatest_2.12" % "3.0.5" % "test"
    ).dependsOn(rbt)

lazy val rbt = project in file("redblacktree")