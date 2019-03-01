name := "sstdemo"
organization := "Gaufoo"

libraryDependencies ++= Seq(
  "org.slf4s" %% "slf4s-api" % "1.7.25",
  "ch.qos.logback" % "logback-classic" % "1.1.2"
)
libraryDependencies += "org.mpierce.metrics.reservoir" % "hdrhistogram-metrics-reservoir" % "1.1.0"