package com.gaufoo.benchmark.utils

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import com.gaufoo.{DelOp, GetOp, Op, SetOp}
import org.slf4s.Logging

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

object FakeDataGenerator extends Logging {
  def prepare(implicit ec: ExecutionContext): Future[Unit] = {
    Future {
      if (Files.notExists(Paths.get("resources/data"))) {
        Files.createDirectory(Paths.get("resources/data"))
      }

      val l = List(50000, 100000, 250000, 500000, 1000000, 2000000)
      l.foreach{ i =>
        genSet(i)
        genGetSet(i)
      }
    }
  }

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
    val writer = new PrintWriter(Paths.get(s"resources/data/set-$number.txt").toFile)

    for {_ <- 1 to number} {
      writer.println(s"set ${randomString(10)} ${randomString(30)}")
    }

    writer.close()
  }

  def genGetSet(number: Int): Unit = {
    val writer = new PrintWriter(Paths.get(s"resources/data/get-set-$number.txt").toFile)

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

  def main(args: Array[String]): Unit = {
    log.debug("Begin data generation")
    Await.result(FakeDataGenerator.prepare(ExecutionContext.global), Duration.Inf)
    log.debug("End data generation")
  }
}
