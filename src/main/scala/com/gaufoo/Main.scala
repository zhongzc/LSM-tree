package com.gaufoo

import com.gaufoo.benchmark.latency.LatencyBenchmark
import com.gaufoo.benchmark.throughput.ThroughputBenchmark
import com.gaufoo.benchmark.utils._
import com.gaufoo.sst.SSTEngine

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

object Main extends App {
  Await.result(jvmWarmUp(ExecutionContext.global), Duration.Inf)

//  ThroughputBenchmark.runBenchmark(getOps())
  LatencyBenchmark.runBenchmark(getOps())
}
