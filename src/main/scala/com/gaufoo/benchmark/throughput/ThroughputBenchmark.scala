package com.gaufoo.benchmark.throughput

import com.gaufoo.sst.SSTEngine
import com.gaufoo.{DelOp, GetOp, Op, SetOp}

object ThroughputBenchmark {
  def runBenchmark(ops: List[Op]): Unit = {

    val s = SSTEngine.build("throughput")

    val start = System.currentTimeMillis()
    ops foreach {
      case SetOp(k, v) => s.set(k, v)
      case GetOp(k)    => s.get(k)
      case DelOp(k)    => s.delete(k)
    }
    val end = System.currentTimeMillis()
    val delayInSeconds = (end - start) / 1000.0

    println {
      s"""|Processed ${ops.size} commands
          |in $delayInSeconds seconds
          |Throughput: ${ops.size / delayInSeconds} operations/sec
     """.stripMargin
    }

    s.shutdown()
  }
}
