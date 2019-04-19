package com.gaufoo.collection.immutable

import com.gaufoo.collection.immutable.{RedBlackTree => RB}

final class TreeMap[K, V] private(tree: RB.Tree[K, V])(implicit val ordering: Ordering[K]) {

  def this()(implicit ordering: Ordering[K]) = this(RB.Tree.empty[K, V])(ordering)

  def size: Int = tree.size

  def height: Int = tree.height

  def get(key: K): Option[V] = RB.Tree.get(key, tree)

  def apply(key: K): V = get(key).get

  def insert(key: K, value: V): TreeMap[K, V] = new TreeMap(RB.Tree.update(key, value, tree)(ordering))

  def remove(key: K): TreeMap[K, V] = new TreeMap(RB.Tree.delete(key, tree)(ordering)._1)

  def map[B](fn: (K, V) => B): List[B] = mapAsc(fn)

  def mapInRange[B](left: K, right: K, fn: (K, V) => B): List[B] = mapInRangeAsc(left, right, fn)

  def foreach(fn: (K, V) => Unit): Unit = map { case (k, v) => fn(k, v) }

  def mapAsc[B](fn: (K, V) => B): List[B] = RB.Tree.map(fn, tree)

  def mapDes[B](fn: (K, V) => B): List[B] = RB.Tree.map(fn, tree, true)

  def mapInRangeAsc[B](left: K, right: K, fn: (K, V) => B): List[B] = {
    assert(ordering.compare(left, right) <= 0)
    RB.Tree.mapInRange(left, right, fn, tree)(ordering)
  }

  def mapInRangeDes[B](right: K, left: K, fn: (K, V) => B): List[B] = {
    assert(ordering.compare(left, right) <= 0)
    RB.Tree.mapInRange(left, right, fn, tree, true)(ordering)
  }

  def getAllEntries: List[(K, V)] = map { case (k, v) => (k, v) }

  def foldLeft[B](z: B)(op: (B, (K, V)) => B): B = {
    var result = z
    this foreach { case (k, v) => result = op(result, (k, v)) }
    result
  }

  def contains(key: K): Boolean = get(key).isDefined
}

object TreeMap {

  def empty[K, V](implicit ordering: Ordering[K]): TreeMap[K, V] = new TreeMap[K, V]()(ordering)

  def apply[K, V](maps: (K, V)*)(implicit ordering: Ordering[K]): TreeMap[K, V] = {
    maps.foldLeft(empty[K, V](ordering)) { case (map, kv) => map.insert(kv._1, kv._2) }
  }

  def main(args: Array[String]): Unit = {
    var s = collection.immutable.TreeMap.empty[Int, String]
    val s1 = System.currentTimeMillis()
    (1 to 100000).foreach(i => s = s.insert(i, (i * i).toString))
    (1 to 100000).foreach(i => s = s - i)
    val e1 = System.currentTimeMillis()
    println(e1 - s1)

    var m = TreeMap.empty[Int, String]
    val s2 = System.currentTimeMillis()
    (1 to 100000).foreach(i => m = m.insert(i, (i * i).toString))
    (1 to 100000).foreach(i => m = m.remove(i))
//    println(m.mapInRangeDes(1000, 0, {case (l, r) => (l, r)}).mkString(", "))
    val e2 = System.currentTimeMillis()
    println(e2 - s2)
  }
}