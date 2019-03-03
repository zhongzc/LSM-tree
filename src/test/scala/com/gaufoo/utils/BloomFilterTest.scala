package com.gaufoo.utils

import org.scalatest.{FlatSpec, Matchers}
import org.slf4s.Logging

import scala.collection.mutable
import scala.util.Random

class BloomFilterTest extends FlatSpec with Logging with Matchers {
  private def randomString(maxLength: Int): String = {
    val l = Random.nextInt(maxLength + 1)
    val result = new mutable.StringBuilder()
    for (_ <- 1 to l) {
      result.append(Random.nextPrintableChar())
    }
    result.toString()
  }

  "BloomFilter" should "be able to work" in {
    val maxLength = 50

    val bloomFilter = new BloomFilter(300, 5)

    val dataSet = (1 to 50).map(_ => randomString(maxLength))
    dataSet.foreach(bloomFilter.set)
    val unrelatedDataSet = (1 to 50).map(_ => randomString(maxLength))
    val mixedDataSet = dataSet ++ unrelatedDataSet

    var falsePositive = 0
    var total = 0

    mixedDataSet.foreach(s =>
      if (bloomFilter.contains(s)) {
        if (!dataSet.contains(s)) falsePositive += 1
        total += 1
      } else {
        dataSet.contains(s) shouldBe false
      }
    )

    log.debug(s"Theory false positive rate: ${bloomFilter.falsePositiveProbability}")
    log.debug(s"Actual false positive rate: ${falsePositive * 1.0 / total}")

  }
}