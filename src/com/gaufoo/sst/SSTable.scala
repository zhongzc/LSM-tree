package com.gaufoo.sst

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}

import scala.collection.immutable.TreeMap
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class SSTable {

  private type Key = String
  private type Value = String
  private type Offset = Int
  private val UTF_8 = "UTF-8"

  final case class SSTable(fileName: String, indexTree: TreeMap[Key, Offset])
  final case class State(segments: List[SSTable], memoryTable: TreeMap[Key, Value])

  // 初始化状态
  def initState: State = State(List(), TreeMap())

  /**
    * 当前SST状态，包含：
    * segments: 已经持久化于文件中的段，包括文件名、一个从键到文件偏移的映射
    * memoryTable: 内存中的表，包含从键到值的映射
    *
    * 只有一个写进程可以修改state。
    * 读state是线程安全的。
    */
  private var state = initState

  /**
    * 查找特定的键，先从内存表中查找，接下来从段文件中找。
    *
    * @param key 需要查找的键
    * @return 返回查找结果
    */
  def get(key: Key): Future[Option[Value]] = {
    val State(segments, memTable) = state
    getFromMemory(key, memTable).flatMap(value =>
      if (value.isEmpty) {
        getFromSegments(key, segments)
      } else {
        Future(value)
      }
    )
  }

  private def getFromMemory(key: Key, memoryTable: TreeMap[Key, Value]): Future[Option[Value]] = {
    Future(memoryTable.get(key))
  }

  private def getFromSegments(key: Key, segments: List[SSTable]): Future[Option[Value]] = {
    val intLength = 4
    Future {
      segments.dropWhile(!_.indexTree.contains(key)).headOption.map {
        case SSTable(fileName, indexTree) =>
          val byteArray = Files.readAllBytes(Paths.get(fileName))
          val byteBuffer = ByteBuffer.wrap(byteArray)

          // 从索引树中获取值在文件中的偏移
          val offset: Int = indexTree(key)

          // 从偏移处获取键字符串字节长
          val keyLength = byteBuffer.getInt(offset)

          // 读取键字符串
          val keyString = new String(byteArray, offset + intLength, keyLength, UTF_8)

          // 从接下来的位置获取值字符串字节长
          val valueLength = byteBuffer.getInt(offset + intLength + keyLength)

          // 读取值字符串
          val valueString = new String(byteArray, offset + keyLength + intLength * 2, valueLength, UTF_8)

          assert(keyString == key)
          valueString
      }
    }
  }

}

object SSTable {
  def main(args: Array[String]): Unit = {
    new SSTable
  }
}