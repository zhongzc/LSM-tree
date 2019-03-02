package com.gaufoo.benchmark.utils

import com.gaufoo.sst.KVEngine
import scala.concurrent.Future

abstract class Op(val op: String, val params: String*) {
  def sendTo(kvEngine: KVEngine): Future[Any] = this match  {
    case SetOp(k, v) => kvEngine.set(k, v)
    case GetOp(k)    => kvEngine.get(k)
    case DelOp(k)    => kvEngine.delete(k)
  }
}

object Op {
  def lineToOps(line: String): Op = {
    val l = line.split(" ")
    l(0) match {
      case "set" => SetOp(l(1), l(2))
      case "get" => GetOp(l(1))
      case "del" => DelOp(l(1))
    }
  }
}

final case class SetOp(key: String, value: String) extends Op("set", key, value)
final case class GetOp(key: String) extends Op("get", key)
final case class DelOp(key: String) extends Op("del", key)
