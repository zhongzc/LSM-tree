package com.gaufoo.utils.bloomFilter

import com.gaufoo.BasicAsyncFlatSpec
import com.gaufoo.utils.bloomfilter.SimpleBloomFilter

class BloomFilterTest extends BasicAsyncFlatSpec {
  import com.gaufoo.benchmark.utils.FakeDataGenerator.randomString

  "BloomFilter" should "be able to work" in {
    val maxLength = 50

    val bloomFilter = SimpleBloomFilter(5000, 0.01)

    val dataSet = (1 to 5000).map(_ => randomString(maxLength))
    dataSet.foreach(bloomFilter.set)
    val unrelatedDataSet = (1 to 2500).map(_ => randomString(maxLength))
    val mixedDataSet = dataSet ++ unrelatedDataSet

    var falsePositive = 0
    var total = 0

    mixedDataSet.foreach(s =>
      if (bloomFilter.mightContain(s)) {
        if (!dataSet.contains(s)) falsePositive += 1
        total += 1
      } else {
        dataSet.contains(s) shouldBe false
      }
    )

    log.debug(s"Theoretical false positive rate: ${bloomFilter.falsePositiveProbability}")
    log.debug(s"Actual      false positive rate: ${falsePositive * 1.0 / total}")
    succeed
  }
}
