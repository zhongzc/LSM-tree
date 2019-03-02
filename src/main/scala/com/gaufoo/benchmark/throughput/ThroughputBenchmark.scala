package com.gaufoo.benchmark.throughput

import com.gaufoo.benchmark.utils._
import com.gaufoo.sst.KVEngine

object ThroughputBenchmark {
  def runBenchmark(kvEngine: KVEngine, ops: List[Op]): Unit = {

    val start = System.currentTimeMillis()
    ops foreach { _.sendTo(kvEngine) }
    val end = System.currentTimeMillis()
    val delayInSeconds = (end - start) / 1000.0

    println {
      s"""|Processed ${ops.size} commands
          |in $delayInSeconds seconds
          |Throughput: ${ops.size / delayInSeconds} operations/sec
     """.stripMargin
    }
  }
}
