package com.gaufoo

import java.io.PrintWriter
import java.nio.file.Paths

import com.gaufoo.sst.{KeyValueMap, SSTEngine}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

abstract class Op(val op: String, val params: String*)
final case class SetOp(key: String, value: String) extends Op("set", key, value)
final case class GetOp(key: String) extends Op("get", key)
final case class DelOp(key: String) extends Op("del", key)

object Main extends App {
  val size = 500000
  val ops = "get-set"

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

object Util {
  def randomString(maxLength: Int): String = {
    val r = Random
    val l = r.nextInt(maxLength) + 1
    val sb = new StringBuilder
    for (_ <- 1 to l) {
      sb.append(r.nextPrintableChar)
    }
    sb.toString
  }

  def genSet(number: Int): Unit = {
    val writer = new PrintWriter(Paths.get(s"resources/set-$number.txt").toFile)

    for {_ <- 1 to number} {
      writer.println(s"set ${randomString(10)} ${randomString(30)}")
    }

    writer.close()
  }

  def genGetSet(number: Int): Unit = {
    val writer = new PrintWriter(Paths.get(s"resources/get-set-$number.txt").toFile)

    val buffer = ArrayBuffer[Op]()
    buffer += SetOp(randomString(10), randomString(30))

    for (i <- 1 until number) {
      if (Random.nextBoolean()) {
        buffer += SetOp(randomString(10), randomString(30))
      } else {
        buffer += GetOp(buffer(Random.nextInt(i)).params(0))
      }
    }

    buffer.foreach{
      case SetOp(key, value) => writer.println(s"set $key $value")
      case GetOp(key)        => writer.println(s"get $key")
      case DelOp(key)        => writer.println(s"del $key")
    }

    writer.close()
  }
}