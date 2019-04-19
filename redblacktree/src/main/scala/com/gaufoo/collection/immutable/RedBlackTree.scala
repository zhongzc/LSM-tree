package com.gaufoo.collection.immutable

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @author Gaufoo(zhongzc_arch@outlook.com)
  * @date 2019/03/11
  * @see [http://matt.might.net/articles/red-black-delete/]
  */
private[collection] object RedBlackTree {

  object Tree {
    def empty[K, V]: Tree[K, V] = Ety

    def update[K, V](key: K, value: V, tree: Tree[K, V])(implicit ord: Ordering[K]): Tree[K, V] = {
      def ins(tree: Tree[K, V]): Tree[K, V] = tree match {
        case Ety => RBTree(Red, Ety, KV(key, value), Ety)
        case RBTree(color, a, y, b) =>
          val comp = ord.compare(key, y.key)
          if (comp < 0) balance(color, ins(a), y, b)
          else if (comp > 0) balance(color, a, y, ins(b))
          else RBTree(color, a, KV(key, value), b)
        case _ => sys.error("non gettable")
      }

      ins(tree).blacken
    }

    def delete[K, V](key: K, tree: Tree[K, V])(implicit ord: Ordering[K]): (Tree[K, V], Option[V]) = {
      def del(tree: Tree[K, V]): (Tree[K, V], Option[V]) = tree match {
        case Ety => (Ety, None)
        case s@RBTree(color, a, y, b) =>
          val comp = ord.compare(key, y.key)
          if (comp < 0) {
            val (t, v) = del(a)
            (bubble(color, t, y, b), v)
          } else if (comp > 0) {
            val (t, v) = del(b)
            (bubble(color, a, y, t), v)
          } else {
            (remove(s), Option(y.value))
          }
        case _ => sys.error("non gettable")
      }

      val (t, v) = del(tree)
      (t.blacken, v)
    }

    @tailrec
    def get[K, V](key: K, tree: Tree[K, V])(implicit ord: Ordering[K]): Option[V] = {
      tree match {
        case Ety => None
        case RBTree(_, l, y, r) =>
          val comp = ord.compare(key, y.key)
          if (comp < 0) get(key, l)
          else if (comp > 0) get(key, r)
          else Option(y.value)
        case _ => sys.error("non gettable")
      }
    }

    def map[K, V, B](func: (K, V) => B, tree: Tree[K, V], des: Boolean = false): List[B] = {
      val remain = mutable.ArrayStack(tree)
      val listBuf = ListBuffer.empty[B]
      while (remain.nonEmpty) {
        val head = remain.pop()
        if (!des) {
          head match {
            case RBTree(_, Ety, kv, Ety) =>
              listBuf += func(kv.key, kv.value)
            case RBTree(_, left, kv, Ety) =>
              remain.push(RBTree(Black, Ety, kv, Ety))
              remain.push(left)
            case RBTree(_, Ety, kv, right) =>
              listBuf += func(kv.key, kv.value)
              remain.push(right)
            case RBTree(_, left, kv, right) =>
              remain.push(right)
              remain.push(RBTree(Black, Ety, kv, Ety))
              remain.push(left)

            case Ety =>
            case _ => sys.error("non gettable")
          }
        } else {
          head match {
            case RBTree(_, Ety, kv, Ety) =>
              listBuf += func(kv.key, kv.value)
            case RBTree(_, left, kv, Ety) =>
              listBuf += func(kv.key, kv.value)
              remain.push(left)
            case RBTree(_, Ety, kv, right) =>
              remain.push(RBTree(Black, Ety, kv, Ety))
              remain.push(right)
            case RBTree(_, left, kv, right) =>
              remain.push(left)
              remain.push(RBTree(Black, Ety, kv, Ety))
              remain.push(right)

            case Ety =>
            case _ => sys.error("non gettable")
          }
        }
      }
      listBuf.toList
    }

    def mapInRange[K, V, B](left: K, right: K, func: (K, V) => B, tree: Tree[K, V], des: Boolean = false)(implicit ord: Ordering[K]): List[B] = {
      map(func, subTreeInRange(left, right, tree)(ord), des)
    }

    private def subTreeInRange[K, V](left: K, right: K, tree: Tree[K, V])(implicit ord: Ordering[K]): Tree[K, V] = tree match {
      case RBTree(_, l, kv, r) =>
        if (ord.compare(kv.key, left) < 0) subTreeInRange(left, right, r)(ord)
        else if (ord.compare(kv.key, left) == 0) RBTree(Black, Ety, kv, subTreeInRange(left, right, r))
        else if (ord.compare(kv.key, right) > 0) subTreeInRange(left, right, l)(ord)
        else if (ord.compare(kv.key, right) == 0) RBTree(Black, subTreeInRange(left, right, l)(ord), kv, Ety)
        else RBTree(Black, subTreeInRange(left, right, l)(ord), kv, subTreeInRange(left, right, r)(ord))

      case Ety => Ety
    }

    private def remove[K, V](tree: Tree[K, V]): Tree[K, V] = tree match {
      case Ety => Ety
      case RBTree(Red, Ety, _, Ety) => Ety
      case RBTree(Black, Ety, _, Ety) => DblEty
      case RBTree(Black, Ety, _, RBTree(Red, a, x, b)) => RBTree(Black, a, x, b)
      case RBTree(Black, RBTree(Red, a, x, b), _, Ety) => RBTree(Black, a, x, b)
      case RBTree(color, l, _, r) =>
        val m = max(l)
        val nl = removeMax(l)
        bubble(color, nl, m, r)
      case _ => sys.error("non gettable")
    }

    private def removeMax[K, V](tree: Tree[K, V]): Tree[K, V] = tree match {
      case Ety => sys.error("no maximum to remove")
      case s@RBTree(_, _, _, Ety) => remove(s)
      case RBTree(color, l, x, r) => bubble(color, l, x, removeMax(r))
      case _ => sys.error("non gettable")
    }

    @tailrec
    private def max[K, V](tree: Tree[K, V]): KV[K, V] = tree match {
      case Ety => sys.error("no largest element")
      case RBTree(_, _, x, Ety) => x
      case RBTree(_, _, _, r) => max(r)
      case _ => sys.error("non gettable")
    }

    // `balance` rotates away coloring conflicts:
    private def balance[K, V](color: Color, left: Tree[K, V], payload: KV[K, V], right: Tree[K, V]): Tree[K, V] =
      (color, left, payload, right) match {

        // Okasaki's original cases:
        case (Black, RBTree(Red, RBTree(Red, a, x, b), y, c), z, d) =>
          RBTree(Red, RBTree(Black, a, x, b), y, RBTree(Black, c, z, d))
        case (Black, RBTree(Red, a, x, RBTree(Red, b, y, c)), z, d) =>
          RBTree(Red, RBTree(Black, a, x, b), y, RBTree(Black, c, z, d))
        case (Black, a, x, RBTree(Red, RBTree(Red, b, y, c), z, d)) =>
          RBTree(Red, RBTree(Black, a, x, b), y, RBTree(Black, c, z, d))
        case (Black, a, x, RBTree(Red, b, y, RBTree(Red, c, z, d))) =>
          RBTree(Red, RBTree(Black, a, x, b), y, RBTree(Black, c, z, d))

        // Six cases for deletion:
        case (DblBlack, RBTree(Red, RBTree(Red, a, x, b), y, c), z, d) =>
          RBTree(Black, RBTree(Black, a, x, b), y, RBTree(Black, c, z, d))
        case (DblBlack, RBTree(Red, a, x, RBTree(Red, b, y, c)), z, d) =>
          RBTree(Black, RBTree(Black, a, x, b), y, RBTree(Black, c, z, d))
        case (DblBlack, a, x, RBTree(Red, RBTree(Red, b, y, c), z, d)) =>
          RBTree(Black, RBTree(Black, a, x, b), y, RBTree(Black, c, z, d))
        case (DblBlack, a, x, RBTree(Red, b, y, RBTree(Red, c, z, d))) =>
          RBTree(Black, RBTree(Black, a, x, b), y, RBTree(Black, c, z, d))
        case (DblBlack, a, x, RBTree(NegBlack, RBTree(Black, b, y, c), z, d@RBTree(Black, _, _, _))) =>
          RBTree(Black, RBTree(Black, a, x, b), y, balance(Black, c, z, d.redden))
        case (DblBlack, RBTree(NegBlack, a@RBTree(Black, _, _, _), x, RBTree(Black, b, y, c)), z, d) =>
          RBTree(Black, balance(Black, a.redden, x, b), y, RBTree(Black, c, z, d))

        case (c, a, x, b) => RBTree(c, a, x, b)
      }

    // `bubble` "bubbles" double-blackness upward:
    private def bubble[K, V](color: Color, left: Tree[K, V], payload: KV[K, V], right: Tree[K, V]): Tree[K, V] =
      if (left.isDoubleBlack || right.isDoubleBlack) {
        balance(color.blacker, left.redder, payload, right.redder)
      } else {
        balance(color, left, payload, right)
      }
  }

  sealed trait Color {
    def blacker: Color

    def redder: Color
  }

  case object Red extends Color {
    override def blacker: Color = Black

    override def redder: Color = NegBlack
  }

  case object Black extends Color {
    override def blacker: Color = DblBlack

    override def redder: Color = Red
  }

  case object DblBlack extends Color { // double black
    override def blacker: Color = sys.error("too black")

    override def redder: Color = Black
  }

  case object NegBlack extends Color { // negative black
    override def blacker: Color = Red

    override def redder: Color = sys.error("not black enough")
  }

  final case class KV[+K, +V](key: K, value: V)

  sealed abstract class Tree[+K, +V] {
    def size: Int

    def height: Int

    def redden: Tree[K, V]

    def blacken: Tree[K, V]

    def isDoubleBlack: Boolean

    def blacker: Tree[K, V]

    def redder: Tree[K, V]
  }

  case object Ety extends Tree[Nothing, Nothing] {
    override val size: Int = 0
    override val height: Int = 0

    override def redden: Tree[Nothing, Nothing] = sys.error("cannot redden empty tree")

    override def blacken: Tree[Nothing, Nothing] = Ety

    override def isDoubleBlack: Boolean = false

    override def blacker: Tree[Nothing, Nothing] = DblEty

    override def redder: Tree[Nothing, Nothing] = sys.error("not black enough")
  } // black leaf
  case object DblEty extends Tree[Nothing, Nothing] {
    override val size: Int = 0
    override val height: Int = 0

    override def redden: Tree[Nothing, Nothing] = sys.error("cannot redden empty tree")

    override def blacken: Tree[Nothing, Nothing] = DblEty

    override def isDoubleBlack: Boolean = true

    override def blacker: Tree[Nothing, Nothing] = sys.error("too black")

    override def redder: Tree[Nothing, Nothing] = Ety
  } // double black leaF
  final case class RBTree[+K, +V](
                                   color: Color,
                                   left: Tree[K, V],
                                   payload: KV[K, V],
                                   right: Tree[K, V]) extends Tree[K, V] {
    override val size: Int = left.size + right.size + 1
    override val height: Int = math.max(left.height, right.height) + 1

    override def redden: Tree[K, V] = RBTree(Red, left, payload, right)

    override def blacken: Tree[K, V] = RBTree(Black, left, payload, right)

    override def isDoubleBlack: Boolean = color == DblBlack

    override def blacker: Tree[K, V] = RBTree(color.blacker, left, payload, right)

    override def redder: Tree[K, V] = RBTree(color.redder, left, payload, right)
  }

}
