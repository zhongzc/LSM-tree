package com.gaufoo

import com.gaufoo.sst.KVEngine

object Main extends App {
  import scala.concurrent.ExecutionContext.Implicits.global

  val kvMap = KVEngine.default("sample")
  kvMap.set("key1", "value1").foreach(_ =>
    kvMap.get("key1").foreach { case Some(v) =>
      println(s"get $v, expected value1, passed: ${v == "value1"}")
      kvMap.shutdown()
    }
  )
}
