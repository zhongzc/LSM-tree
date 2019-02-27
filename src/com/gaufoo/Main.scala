package com.gaufoo

import com.gaufoo.sst.{KeyValueMap, SSTable}
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  val s: KeyValueMap = new SSTable
  s.set("abc", "def").foreach(v => {
      println(v)
      s.get("abc").foreach(println)
    }
  )
}
