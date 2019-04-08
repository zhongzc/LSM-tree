package com.gaufoo

import com.gaufoo.sst.KVEngine

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends App {
  val kvMap = KVEngine.default("sample")
  val v = Await.result(kvMap.set("key1", "value1"), Duration.Inf)
  val v2 = Await.result(kvMap.get("key1"), Duration.Inf)
  val v3 = Await.result(kvMap.delete("key1"), Duration.Inf)
  val v4 = Await.result(kvMap.delete("key1"), Duration.Inf)
  kvMap.shutdown()
  println(v)
  println(v2)
  println(v3)
  println(v4)
}
