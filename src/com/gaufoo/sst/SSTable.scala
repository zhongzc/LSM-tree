package com.gaufoo.sst

import java.nio.ByteBuffer
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import scala.collection.immutable.TreeMap
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

trait KeyValueMap {
  type Key = String
  type Value = String
  def set(key: Key, value: Value): Future[Value]
  def get(key: Key): Future[Option[Value]]
  def delete(key: Key): Future[Option[Value]]
}

class SSTable extends KeyValueMap {
  private type Offset = Int
  private val UTF_8 = "UTF-8"
  private val storingLocation = "."
  private val tombstone = new String(Array[Byte](0, 1, 2, 3, 2, 1, 0, 1))
  lazy val genId: () => Int = {
    var id = -1
    val inner = () => { id = id + 1;  id }
    inner
  }

  final case class MemoryTree(id: Int, tree: TreeMap[Key, Value])
  final case class SSTable(id: Int, fileName: String, indexTree: TreeMap[Key, Offset])

  final case class State(segments: List[SSTable], memoryTrees: List[MemoryTree])

  // 初始化状态
  def initState: State = State(List(), List(MemoryTree(genId(), TreeMap[Key, Value]())))

  /**
    * 当前SST状态，包含：
    * segments: 已经持久化于文件中的段，包括文件名、一个从键到文件偏移的映射
    * memoryTrees: 内存中的表，包含从键到值的映射，正常情况下只有一个，在等待创建段文件的过程中有可能出现多个
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
    val State(segments, memTables) = state
    getFromMemory(key, memTables).flatMap(value =>
      (if (value.isEmpty) {
        getFromSegments(key, segments)
      } else {
        Future(value)
      }).map(ov => ov.flatMap(v => if (v == tombstone) None else Option(v)))
    )
  }

  def delete(key: Key): Future[Option[Value]] = {
    for {
      ov <- get(key)
      _ <- set(key, tombstone)
    } yield ov
  }

  private def getFromMemory(key: Key, memoryTrees: List[MemoryTree]): Future[Option[Value]] = {
    Future {
      memoryTrees.map(_.tree).find(_.contains(key)).map(_(key))
    }
  }

  private def getFromSegments(key: Key, segments: List[SSTable]): Future[Option[Value]] = {
    val intLength = 4
    Future {
      segments.find(_.indexTree.contains(key)).map {
        case SSTable(_, fileName, indexTree) =>
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

  sealed trait Command
  final case class SetKey(key: Key, value: Value, callback: Value => Unit) extends Command
  final case class UpdateSegments(toAdd: List[SSTable], toRemove: List[SSTable]) extends Command
  final case class AddSegment(toAdd: SSTable, toRemove: MemoryTree) extends Command
  final case class Compact(segments: List[SSTable]) extends Command

  val commandQueue: BlockingQueue[Command] = new LinkedBlockingQueue[Command]()

  /**
    * 设置键值对，若键已存在，新值将覆盖旧值
    *
    * @param key 待设置键
    * @param value 待设置值
    * @return 成功设置的值
    */
  def set(key: Key, value: Value): Future[Value] = {
    val f = Promise[Value]()

    commandQueue.put(SetKey(key, value, { v =>
      f.success(v)
    }))

    f.future
  }

  /**
    * 唯一可以修改state的工作线程
    * 其他进程可以通过发送命令到命令队列上，此线程将按序处理命令
    *
    * 接受多种命令，包括：
    * `SetKey`: 设置键值对
    * `UpdateSegments`: 更新state中的段文件
    * `AddSegment`: 在内存转化成文件完成后，更新state中的段文件
    * `Compact`: 压缩SST
    *
    */
  private val worker: Runnable = () => {
    val treeSizeThreshold = 5

    while (true) {
      val op: Command = commandQueue.take()

      op match {
        case SetKey(key, value, callback) => setKey(key, value, callback)

        case UpdateSegments(toAdd, toRemove) => updateSegments(toAdd, toRemove.map(_.id).toSet)

        case AddSegment(toAdd, toRemove) => addSegment(toAdd, toRemove)

        case Compact(segments) => compact(segments)
      }
    }

    def setKey(key: Key, value: Value, callback: Value => Unit): Unit = {
      val State(oldSegments, MemoryTree(id, oldTree) :: others) = state
      val newTree = MemoryTree(id, oldTree.updated(key, value))
      var trees = newTree :: others

      if (newTree.tree.size >= treeSizeThreshold) {
        convertToSeg(newTree)
        trees = MemoryTree(genId(), TreeMap[Key, Value]()) :: trees
      }

      state = State(oldSegments, trees)
      callback(value)
    }

    def convertToSeg(memoryTree: MemoryTree): Future[Unit] = {
      Future {
        val MemoryTree(id, tree) = memoryTree
        val (listOfArray, _, indexTree) = tree.foldLeft((Vector.empty[Array[Byte]], 0, TreeMap.empty[Key, Offset])) {
          case ((acc, offset, iTree), (key, value)) =>
            val bfk = ByteBuffer.allocate(4)
            val bfv = ByteBuffer.allocate(4)

            val keyBytes = key.getBytes(UTF_8)
            val valueBytes = value.getBytes(UTF_8)
            val accOffset = 4 + keyBytes.length + 4 + valueBytes.length + offset
            (acc :+ bfk.putInt(keyBytes.length).array() :+ keyBytes :+
              bfv.putInt(valueBytes.length).array() :+ valueBytes,
              accOffset,
              iTree.updated(key, offset))
        }
        val bytes = listOfArray.toArray.flatten

        val fileName = Files.write(Paths.get(storingLocation).resolve(s"sst-$id"), bytes,
          StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).toString

        commandQueue.put(AddSegment(SSTable(id, fileName, indexTree), memoryTree))
      }
    }

    def compact(segments: List[SSTable]): Future[Unit] = {
      Future {
        // TODO
        commandQueue.put(UpdateSegments(List(), List()))
      }
    }

    def updateSegments(toAdd: List[SSTable], toRemoveIds: Set[Int]): Unit = {
      val State(oldSegments, oldMemoryTrees) = state
      state = State(toAdd ++ oldSegments.filterNot(s => toRemoveIds.contains(s.id)), oldMemoryTrees)
    }

    lazy val addSegment: (SSTable, MemoryTree) => Unit = {
      var blocking = Map[Int, (SSTable, MemoryTree)]()

      // 保证按顺序转化成文件
      val inner = (toAdd: SSTable, toRemove: MemoryTree) => {
        blocking = blocking.updated(toRemove.id, (toAdd, toRemove))

        val State(oldSegments, oldMemoryTrees) = state
        val idsToRemove = oldMemoryTrees.map(_.id).reverse.takeWhile(blocking.isDefinedAt)
        val (ss, _) = idsToRemove.map(blocking).reverse.unzip
        state = State(ss ++ oldSegments, oldMemoryTrees.filterNot(t => idsToRemove.contains(t.id)))
        blocking = blocking -- idsToRemove
      }

      inner
    }
  }

  new Thread(worker).start()
}

