package com.gaufoo

import com.gaufoo.benchmark.FakeDataGenerator
import com.gaufoo.sst.{KeyValueMap, SSTEngine}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.io.Source

abstract class Op(val op: String, val params: String*)
final case class SetOp(key: String, value: String) extends Op("set", key, value)
final case class GetOp(key: String) extends Op("get", key)
final case class DelOp(key: String) extends Op("del", key)

object Main extends App {
  Await.result(FakeDataGenerator.prepare(ExecutionContext.global), Duration.Inf)

  val size = 2000000
  val ops = "set"

  val ol = Source.fromFile(s"resources/$ops-$size.txt").getLines.map(_.split(" ")).map(l => l(0) match {
    case "set" => SetOp(l(1), l(2))
    case "get" => GetOp(l(1))
    case "del" => DelOp(l(1))
  })

  val s: KeyValueMap = SSTEngine.build("throughout")

  val start = System.currentTimeMillis()
  for (o <- ol) {
    o match {
      case SetOp(k, v) => s.set(k, v)
      case GetOp(k)    => s.get(k)
      case DelOp(k)    => s.delete(k)
    }
  }
  val end = System.currentTimeMillis()
  val delayInSeconds = (end - start) / 1000.0

  println {
    s"""|Processed $size commands
        |in $delayInSeconds seconds
        |Throughput: ${size / delayInSeconds} operations/sec
     """.stripMargin
  }

}
