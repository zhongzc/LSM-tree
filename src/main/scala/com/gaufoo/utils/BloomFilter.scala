package com.gaufoo.utils

import java.util.concurrent.atomic.AtomicInteger

/**
  * bloom filter，概率数据结构，用于判断给定的key(字符串)是否被添加过到数据集中。
  * 默认使用String本身的hashCode()。
  *
  * @param m 长度为m的序列
  * @param k 每个添加的元素都计算出k个索引值，通常，k应远小于m
  */
class BloomFilter(m: Int, k: Int, hashFunction: String => Int = s => s.hashCode) {
  private[this] val seq: Array[Boolean] = (0 until m).map(_ => false).toArray
  private[this] val initialValues = 0 until k
  private[this] val counter = new AtomicInteger(0)

  private def indexSeqOf(key: String): Seq[Int] = {
    initialValues.map(initVal => Math.abs(hashFunction(key) + initVal.hashCode()) % m)
  }

  def set(key: String): Unit = {
    indexSeqOf(key).foreach(i => seq(i) = true)
    counter.getAndIncrement()
  }

  /**
    * 判断给定的key是否已添加过到数据集中。
    * @return 如果该key__可能__存在于数据集中，返回true;<br/>
    *         如果该key__一定__不存在于数据集中，返回false;
    */
  def contains(key: String): Boolean = {
    indexSeqOf(key).forall(i => seq(i))
  }

  def size: Int = counter.get()

  /**
    * 计算当前bloom filter对命题"数据集中存在某个键"的错误预测率。
    * @return 区间为[0, 1]的概率
    */
  def falsePositiveProbability: Double = {
    import Math._
    pow(1 - pow(E, -k * counter.get() * 1.0 / m), k)
  }
}
