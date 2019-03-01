package com.gaufoo.benchmark

import com.codahale.metrics.Snapshot
import com.gaufoo.{DelOp, GetOp, Op, SetOp}
import com.gaufoo.sst.SSTEngine
import org.slf4s.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

package object utils extends Logging {
  def jvmWarmUp(implicit ec: ExecutionContext): Future[Unit] = {
    Future {
      log.debug("Begin warm up")

      val t = SSTEngine.build("warm")
      val ol = Source.fromFile("resources/data/get-set-50000.txt").getLines.map(_.split(" ")).map(l => l(0) match {
        case "set" => SetOp(l(1), l(2))
        case "get" => GetOp(l(1))
        case "del" => DelOp(l(1))
      })

      ol foreach {
        case SetOp(k, v) => Await.result(t.set(k, v), Duration.Inf)
        case GetOp(k)    => Await.result(t.get(k), Duration.Inf)
        case DelOp(k)    => Await.result(t.delete(k), Duration.Inf)
      }

      t.shutdown()

      log.debug("End warm up")
    }
  }

  def printSnapshot(s: Snapshot): Unit = println {
    s"""
       |Processed ${s.size} commands
       |95p latency: ${s.get95thPercentile()} ms
       |99p latency: ${s.get99thPercentile()} ms
       |99.9p latency: ${s.get999thPercentile()} ms
       |Maximum latency: ${s.getMax} ms
        """.stripMargin
  }

  def getOps(size: Int = 500000, ops: String = "get-set"): List[Op] = {
    Source.fromFile(s"resources/data/$ops-$size.txt").getLines.map(_.split(" ")).map(l => l(0) match {
      case "set" => SetOp(l(1), l(2))
      case "get" => GetOp(l(1))
      case "del" => DelOp(l(1))
    }).toList
  }
}
