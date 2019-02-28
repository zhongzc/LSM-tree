package com.gaufoo.benchmark

import com.gaufoo.Op

object LatencyBenchmark {

  case class CommandsPerSecond(value: Int) extends AnyVal
  case class BenchmarkIterationCount(value: Int) extends AnyVal
  case class CommandSentTimestamp(value: Long) extends AnyVal

  def runBenchmark(
    sampleCommands: List[Op],
    cps: CommandsPerSecond,
    count: BenchmarkIterationCount): Unit = {

  }
}
