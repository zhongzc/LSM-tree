package com.gaufoo.utils.bloomfilter

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

/**
  * bloom filter，概率数据结构，用于判断给定的key(字符串)是否被添加过到数据集中。
  * 默认使用String本身的hashCode()。
  *
  * @param m 长度为m的序列
  * @param numberOfHashes 每个添加的元素都计算出k个索引值，通常，k应远小于m
  */
class SimpleBloomFilter(m: Int, numberOfHashes: Int, hashFunction: String => Int = s => s.hashCode) extends BloomFilter {
  private[this] val seq: mutable.BitSet = new mutable.BitSet(m)
  private[this] val counter = new AtomicInteger(0)

  private def indexSeqOf(key: String): Seq[Int] = {
    val hash = hashFunction(key)
    val hash1 = hash >>> 16
    val hash2 = (hash << 16) >> 16

    (0 until numberOfHashes).map(i => ((hash1 * i + hash2) & Int.MaxValue) % m)
  }

  override def set(key: String): Unit = {
    indexSeqOf(key).foreach(i => seq(i) = true)
    counter.getAndIncrement()
  }

  override def mightContain(key: String): Boolean = {
    indexSeqOf(key).forall(i => seq(i))
  }

  def size: Int = counter.get()

  /**
    * 计算当前bloom filter对命题"数据集中存在某个键"的错误预测率。
    * @return 区间为[0, 1]的概率
    */
  def falsePositiveProbability: Double = {
    import Math._
    pow(1 - pow(E, -numberOfHashes * counter.get() * 1.0 / m), numberOfHashes)
  }
}

object SimpleBloomFilter {
  def apply(numberOfItems: Int, falsePositiveRate: Double): SimpleBloomFilter = {

    // 通过给定的数量和false positive rate，计算出需要多大的容量和哈希次数
    val sz = optimalSize(numberOfItems, falsePositiveRate)
    val nh = optimalNumberOfHashes(numberOfItems, sz)

    new SimpleBloomFilter(sz, nh)
  }

  def optimalSize(numberOfItems: Int, falsePositiveRate: Double): Int = {
    math.ceil(-1 * numberOfItems * math.log(falsePositiveRate) / math.log(2) / math.log(2)).toInt
  }

  def optimalNumberOfHashes(numberOfItems: Int, size: Int): Int = {
    math.ceil(size / numberOfItems * math.log(2)).toInt
  }
}
