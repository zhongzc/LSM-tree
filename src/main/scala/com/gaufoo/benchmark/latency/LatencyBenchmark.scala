package com.gaufoo.benchmark.latency

import com.gaufoo.benchmark.utils._
import com.gaufoo.sst.SSTEngine
import com.gaufoo.{DelOp, GetOp, Op, SetOp}
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramReservoir

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext

object LatencyBenchmark {

  case class CommandsPerSecond(value: Int) extends AnyVal
  case class BenchmarkIterationCount(value: Int) extends AnyVal
  case class CommandSentTimestamp(value: Long) extends AnyVal

  def runBenchmark(
    ops: List[Op],
    cps: CommandsPerSecond = CommandsPerSecond(50000)): Unit = {
    val sst = SSTEngine.build("latency")

    @tailrec
    def sendCommands(
      xs: List[(Op, Int)],
      sst: SSTEngine,
      testStart: Long,
      histogram: HdrHistogramReservoir): (SSTEngine, HdrHistogramReservoir) = xs match {

      case head :: tail =>
        val (op, offsetInMs) = head
        val shouldStart = testStart + offsetInMs

        while (shouldStart > System.currentTimeMillis()) {
          // keep the thread busy while waiting for the next batch to be sent
        }

        (op match {
          case SetOp(k, v) => sst.set(k, v)
          case GetOp(k)    => sst.get(k)
          case DelOp(k)    => sst.delete(k)
        }).foreach { _ =>
          val operationEnd = System.currentTimeMillis()
          histogram.update(operationEnd - shouldStart)
        }(ExecutionContext.global)

        sendCommands(tail, sst, testStart, histogram)

      case Nil => (sst, histogram)
    }

    val (_, histogram) = sendCommands(
      ops.grouped(cps.value).toList.zipWithIndex
        .flatMap {
          case (secondBatch, sBatchIndex) =>
            val batchOffsetInMs = sBatchIndex * 1000
            val commandIntervalInMs = 1000.0 / cps.value
            secondBatch.zipWithIndex.map {
              case (command, commandIndex) =>
                val commandOffsetInMs =
                  Math.floor(commandIntervalInMs * commandIndex).toInt
                (command, batchOffsetInMs + commandOffsetInMs)
            }
        },
      sst,
      System.currentTimeMillis(),
      new HdrHistogramReservoir())

    Thread.sleep(2000)
    sst.shutdown()
    printSnapshot(histogram.getSnapshot)
  }
}
