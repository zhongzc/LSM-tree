package com.gaufoo

import com.gaufoo.sst.{KeyValueMap, SSTEngine}
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {
  val s: KeyValueMap = new SSTEngine

  s.set("abc", "def").foreach(v => {
      println(v)
      s.get("abc").foreach(println)
    }
  )

  s.delete("abc").foreach(v => {
    println(v)
    s.get("abc").foreach(println)
  })
}
