package com.gaufoo

import com.gaufoo.benchmark.latency.LatencyBenchmark
import com.gaufoo.benchmark.throughput.ThroughputBenchmark
import com.gaufoo.benchmark.utils._
import com.gaufoo.sst.SSTEngine

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends App {
  val warmup = SSTEngine.build("warmup")
  Await.result(jvmWarmUp(warmup), Duration.Inf)
  warmup.shutdown()

//  val throughput = SSTEngine.build("throughput")
//  ThroughputBenchmark.runBenchmark(throughput, getOps())
//  throughput.shutdown()

  val latency = SSTEngine.build("latency", 1500)
  LatencyBenchmark.runBenchmark(latency, getOps(250000, "set"))
  latency.shutdown()
}
