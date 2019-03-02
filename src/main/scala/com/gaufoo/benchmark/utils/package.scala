package com.gaufoo.benchmark

import com.codahale.metrics.Snapshot
import com.gaufoo.sst.KVEngine
import org.slf4s.Logging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source

package object utils extends Logging {
  def jvmWarmUp(kvEngine: KVEngine): Future[Unit] = {
    Future {
      log.debug("Begin warm up")

      val ol = Source.fromFile("resources/data/get-set-50000.txt").getLines.map(Op.lineToOps)
      ol foreach { o => Await.result(o.sendTo(kvEngine), Duration.Inf) }

      log.debug("End warm up")
    }(ExecutionContext.global)
  }

  def printSnapshot(s: Snapshot): Unit = println {
    s"""
       |Processed ${s.size} commands
       |95p latency: ${s.get95thPercentile()} ms
       |99p latency: ${s.get99thPercentile()} ms
       |99.9p latency: ${s.get999thPercentile()} ms
       |Maximum latency: ${s.getMax} ms
       |""".stripMargin
  }

  def getOps(size: Int = 500000, ops: String = "get-set"): List[Op] = {
    Source.fromFile(s"resources/data/$ops-$size.txt").getLines.map(Op.lineToOps).toList
  }
}
