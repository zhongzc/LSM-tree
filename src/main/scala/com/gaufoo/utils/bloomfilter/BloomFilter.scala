package com.gaufoo.utils.bloomfilter

trait BloomFilter {
  def set(key: String): Unit
  def mightContain(key: String): Boolean
}

object BloomFilter {
  def default(numberOfItems: Long, falsePositiveRate: Double): BloomFilter =
    SimpleBloomFilter(numberOfItems, falsePositiveRate)
}