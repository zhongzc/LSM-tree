package com.gaufoo.sst

import java.nio.file._

import com.gaufoo.BasicAsyncFlatSpec

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

class SSTEngineTest extends BasicAsyncFlatSpec {

  import BasicAsyncFlatSpec._

  "build" should "create a folder at first" in withTestEngine(bufferSize = 1500) { _ =>
    val file = Paths.get(dbLocation).toFile
    file.exists() shouldBe true
    file should be a 'directory
  }

  "a key-value pair" should "be able to retrieve after being set" in
    withTestEngine(bufferSize = 1500) { engine =>
      val (key, value) = "testKey" -> "testVal"
      Await.result(engine.set(key, value), Duration.Inf)
      val v = Await.result(engine.get(key), Duration.Inf)
      v should equal(Some(value))
    }

  "a key" should "not contained in map after being deleted" in
    withTestEngine(bufferSize = 1500) { engine =>
      val (key, value) = "test1" -> "test2"
      val kvPairs = (1 to 100000).map(i => (i.toString, i.toString))

      for {
        _ <- engine.set(key, value)
        fvs = kvPairs.map { case (k, v) => engine.set(k, v) }
        _ <- Future.sequence(fvs)
        _ <- engine.delete(key)
        result <- engine.get(key)
      } yield result shouldBe empty
    }

  "retrieve a non-exist key" should "return none" in withTestEngine(bufferSize = 1500) { engine =>
    val kvPairs = (1 to 100000).map(i => (i.toString, i.toString))
    val fvs = kvPairs.map { case (k, v) => engine.set(k, v) }

    for {
      _ <- Future.sequence(fvs)
      result <- engine.get("100001")
    } yield result shouldBe empty
  }

  "setting, deleting and getting random data" should "behave normally" in
    withTestEngine(bufferSize = 1500) { engine =>
      val maxLength = 50
      val value = "test"
      val dataSet = (1 to 100000).map(_ => randomString(maxLength)).toSet
      val (deletedDateSet, savedDataSet) = dataSet.splitAt(dataSet.size / 2)

      dataSet.foreach(s => Await.result(engine.set(s, value), Duration.Inf))
      deletedDateSet.foreach(s => Await.result(engine.delete(s), Duration.Inf))
      val ss = savedDataSet.map(s => Await.result(engine.get(s), Duration.Inf))
      val ds = deletedDateSet.map(s => Await.result(engine.get(s), Duration.Inf))

      assert(ss.forall(_.nonEmpty))
      assert(ds.forall(_.isEmpty))

      succeed
    }

  "setting, deleting and getting random number" should "behave normally" in
    withTestEngine(bufferSize = 1500) { engine =>
      var delNums = Set.empty[Int]
      (1 to 300000).foreach(_ => delNums = delNums + Random.nextInt(1000000))

      (1 to 1000000).foreach(i => Await.result(engine.set(i.toString, i.toString), Duration.Inf))
      delNums.foreach(i => Await.result(engine.delete(i.toString), Duration.Inf))

      (1 to 1000000).foreach(i => {
        val num = Await.result(engine.get(i.toString), Duration.Inf)
        if (delNums.contains(i)) assert(num.isEmpty)
        else assert(num.get == i.toString)
      })

      succeed
    }

  "get all keys in ascending order" should "behave normally" in
    withTestEngine(bufferSize = 1500) { engine =>
      var delNums = Set.empty[Int]
      (1 to 300000).foreach(_ => delNums = delNums + Random.nextInt(1000000))

      (1 to 1000000).foreach(i => Await.result(engine.set("%08d".format(i), "%08d".format(i)), Duration.Inf))
      delNums.foreach(i => Await.result(engine.delete("%08d".format(i)), Duration.Inf))

      ((1 to 1000000).toSet -- delNums).toList.sorted.zip(Await.result(engine.allKeysAsc(), Duration.Inf))
        .foreach(a => assert("%08d".format(a._1) == a._2))

      succeed
    }

  "get all keys in descending order" should "behave normally" in
    withTestEngine(bufferSize = 1500) { engine =>
      var delNums = Set.empty[Int]
      (1 to 300000).foreach(_ => delNums = delNums + Random.nextInt(1000000))

      (1 to 1000000).foreach(i => Await.result(engine.set("%08d".format(i), "%08d".format(i)), Duration.Inf))
      delNums.foreach(i => Await.result(engine.delete("%08d".format(i)), Duration.Inf))

      ((1 to 1000000).toSet -- delNums).toList.sorted.reverse.zip(Await.result(engine.allKeysDes(), Duration.Inf))
        .foreach(a => assert("%08d".format(a._1) == a._2))

      succeed
    }

  "get range keys in ascending order" should "behave normally" in
    withTestEngine(bufferSize = 1500) { engine =>
      (1 to 1000000).foreach(i => Await.result(engine.set("%08d".format(i), "%08d".format(i)), Duration.Inf))
      Await.result(engine.rangeKeysAsc("00000100", "00100000"), Duration.Inf).zip(100 to 100000)
        .foreach(a => assert(a._1 == "%08d".format(a._2)))

      succeed
    }

  "get range keys in descending order" should "behave normally" in
    withTestEngine(bufferSize = 1500) { engine =>
      (1 to 1000000).foreach(i => Await.result(engine.set("%08d".format(i), "%08d".format(i)), Duration.Inf))
      Await.result(engine.rangeKeysDes("00100000", "00000100"), Duration.Inf).zip(100000 to 100 by -1)
        .foreach(a => assert(a._1 == "%08d".format(a._2)))

      succeed
    }

  "SSTEngine" should "rebuild indexTree after restart" in {
    removeDbFolderIfExist(dbLocation)
    val engine1 = SSTEngine.build(dbName)

    val maxLength = 50
    val value = "test"
    val dataSet = (1 to 10000).map(_ => randomString(maxLength)).toSet

    Future.sequence(dataSet.map(key => engine1.set(key, value)))
      .flatMap(_ => engine1.shutdown())
      .map(_ => SSTEngine.build(dbName))
      .flatMap(engine2 => {
        Future.sequence(
          dataSet.map(key => engine2.get(key).map(ov => assert(ov.contains(value))))
        ).flatMap(_ => engine2.shutdown())
      })
      .map(_ => removeDbFolderIfExist(dbLocation))
      .map(_ => succeed)
  }
}
