package com.gaufoo.sst

import java.nio.file._

import com.gaufoo.BasicAsyncFlatSpec

import scala.concurrent.Future

class SSTEngineTest extends BasicAsyncFlatSpec {
  import BasicAsyncFlatSpec._

  "build" should "create a folder at first" in withTestEngine(bufferSize = 20) { _ =>
    val file = Paths.get(dbLocation).toFile
    file.exists() shouldBe true
    file should be a 'directory
  }

  "a key-value pair" should "be able to retrieve after being set" in
    withTestEngine(bufferSize = 20) { engine =>
      val (key, value) = "testKey" -> "testVal"
      engine.set(key, value).foreach { _ =>
        engine.get(key).foreach(v =>
          v should equal(Some(value)))
      }
    }

  "a key" should "not contained in map after being deleted" in
    withTestEngine(bufferSize = 20) { engine =>
      val (key, value) = "test1" -> "test2"
      val kvPairs = (1 to 1000).map(i => (i.toString, i.toString))

      for {
        _ <- engine.set(key, value)
        fvs = kvPairs.map{ case (k, v) => engine.set(k, v) }
        _ <- Future.sequence(fvs)
        _ <- engine.delete(key)
        result <- engine.get(key)
      } yield result shouldBe empty
    }

  "retrieve a non-exist key" should "return none" in withTestEngine(bufferSize = 20) { engine =>
    val kvPairs = (1 to 1000).map(i => (i.toString, i.toString))
    val fvs = kvPairs.map { case (k, v) => engine.set(k, v)}

    for {
      _ <- Future.sequence(fvs)
      result <- engine.get("1001")
    } yield result shouldBe empty
  }

  "setting, deleting and getting random data" should "behave normally" in
    withTestEngine(bufferSize = 20) { engine =>
      val maxLength = 50
      val value = "test"
      val dataSet = (1 to 1000).map(_ => randomString(maxLength))
      val (deletedDateSet, savedDataSet) = dataSet.splitAt(500)

      for {
        _ <- Future.sequence(dataSet.map(engine.set(_, value)))
        _ <- Future.sequence(deletedDateSet.map(engine.delete))
        ss <- Future.sequence(savedDataSet.map(engine.get))
        ds <- Future.sequence(deletedDateSet.map(engine.get))
      } yield {
        assert(ss.forall(_.nonEmpty))
        assert(ds.forall(_.isEmpty))
      }
    }

}
