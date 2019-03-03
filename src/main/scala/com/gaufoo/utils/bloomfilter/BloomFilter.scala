package com.gaufoo.utils.bloomfilter

trait BloomFilter {
  /**
    * 将给定的key加入到过滤器中，该key应用[[mightContain]]将返回`true`
    *
    */
  def set(key: String): Unit

  /**
    * 判断给定的key是否已添加过到数据集中。
    * @return 如果该key__可能__存在于数据集中，返回true;<br/>
    *         如果该key__一定__不存在于数据集中，返回false;
    */
  def mightContain(key: String): Boolean
}

object BloomFilter {
  def default(numberOfItems: Int, falsePositiveRate: Double): BloomFilter =
    SimpleBloomFilter(numberOfItems, falsePositiveRate)
}