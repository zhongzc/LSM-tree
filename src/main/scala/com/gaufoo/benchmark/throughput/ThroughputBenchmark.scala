package com.gaufoo.benchmark.throughput

import com.gaufoo.benchmark.utils._
import com.gaufoo.sst.{KVEngine, SSTEngine}
import org.slf4s.Logging

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ThroughputBenchmark extends Logging {
  def runBenchmark(kvEngine: KVEngine, ops: List[Op]): Unit = {
    log.debug("Begin throughput benchmarking")
    val start = System.currentTimeMillis()
    ops foreach { _.sendTo(kvEngine) }
    val end = System.currentTimeMillis()
    val delayInSeconds = (end - start) / 1000.0

    Thread.sleep(2000)
    println {
      s"""|Processed ${ops.size} commands
          |in $delayInSeconds seconds
          |Throughput: ${ops.size / delayInSeconds} operations/sec
          |""".stripMargin
    }

    log.debug("End throughput benchmarking")
  }

  def main(args: Array[String]): Unit = {
    val warmUp = SSTEngine.build("warmUp")
    Await.result(jvmWarmUp(warmUp), Duration.Inf)
    warmUp.shutdown()

    val throughput = SSTEngine.build("throughput")
    ThroughputBenchmark.runBenchmark(throughput, getOps())
    throughput.shutdown()
  }
}
