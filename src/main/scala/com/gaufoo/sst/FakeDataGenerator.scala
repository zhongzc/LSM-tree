package com.gaufoo.sst

import java.io.PrintWriter
import java.nio.file.Paths

import com.gaufoo.{DelOp, GetOp, Op, SetOp}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object FakeDataGenerator {
  def prepare(implicit ec: ExecutionContext): Future[Boolean] = {
    Future {
      val l = List(50000, 100000, 250000, 500000, 1000000, 2000000)
      l.foreach{ i =>
        genSet(i)
        genGetSet(i)
      }

      true
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
