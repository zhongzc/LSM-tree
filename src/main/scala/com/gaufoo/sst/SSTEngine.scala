package com.gaufoo.sst

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent._

import scala.collection.immutable.TreeMap
import scala.concurrent._
import scala.concurrent.Future

object SSTEngine {
  private val UTF_8 = "UTF-8"
  private type Offset = Int
  private val tombstone = new String(Array[Byte](0, 1, 2, 3, 2, 1, 0, 1))

  /**
    * 两种Future执行环境
    * 一种为默认，用于非阻塞，和CPU核数匹配
    * 一种用于阻塞操作，如I/O
    *
    * 另外还有定时任务执行环境，供后台压缩使用
    */

  def build(dbName: String, bufferSize: Int = 1800): SSTEngine = {
    new SSTEngine(dbName, bufferSize)
  }
}

class SSTEngine(dbName: String, bufferSize: Int) extends KVEngine {
  import SSTEngine._

  private[this] val storePath: Path = Paths.get(s"resources/$dbName")

  lazy val genId: () => Int = {
    var id = -1
    val inner = () => { id = id + 1;  id }
    inner
  }

  private[this] final case class MemoryTree(id: Int, tree: TreeMap[Key, Value])
  private[this] final case class SSTable(id: Int, fileName: String, indexTree: TreeMap[Key, Offset])
  private[this] final case class State(segments: List[SSTable], memoryTrees: List[MemoryTree])

  /**
    * 根据数据库文件夹初始化状态，若不存在则创建新数据库
    *
    * @param storePath 数据库文件路径
    * @return 根据快照创建好的状态
    */
  private[this] def initState(storePath: Path): State = {
    if (Files.notExists(storePath)) {
      Files.createDirectory(storePath)
      State(List(), List(MemoryTree(genId(), TreeMap[Key, Value]())))
    } else {

      // TODO: 根据文件夹重新构建索引树
      State(List(), List(MemoryTree(genId(), TreeMap[Key, Value]())))
    }
  }

  /**
    * 当前SST状态，包含：
    * segments: 已经持久化于文件中的段，包括文件名、一个从键到文件偏移的映射
    * memoryTrees: 内存中的表，包含从键到值的映射，正常情况下只有一个，在等待创建段文件的过程中有可能出现多个
    *
    * 只有一个写进程可以修改state。
    * 读state是线程安全的。
    */
  private[this] var state = initState(storePath)

  /**
    * 查找特定的键，先从内存表中查找，接下来从段文件中找。
    *
    * @param key 需要查找的键
    * @return 返回查找结果
    */
  override def get(key: Key): Future[Option[Value]] = {
    // TODO: 用Bloom Filter预过滤一遍

    val State(segments, memTables) = state
    getFromMemory(key, memTables).flatMap(value =>
      (if (value.isEmpty) {
        getFromSegments(key, segments)
      } else {
        Future.successful(value)
      }).map(ov => ov.flatMap(v => if (v == tombstone) None else Option(v)))
    )
  }

  /**
    * 删除某键，为了简单利用`tombstone`占位，压缩时才清除
    *
    * @param key 需要删除的键
    * @return 键原有的值
    */
  override def delete(key: Key): Future[Option[Value]] = {
    for {
      ov <- get(key)
      _ <- set(key, tombstone)
    } yield ov
  }

  private[this] def getFromMemory(key: Key, memoryTrees: List[MemoryTree]): Future[Option[Value]] = {
    Future {
      memoryTrees.map(_.tree).find(_.contains(key)).map(_(key))
    }
  }

  private[this] def getFromSegments(key: Key, segments: List[SSTable]): Future[Option[Value]] = {
    val intLength = 4
    Future {
      // TODO: 稀疏
      // 先从内存中的索引树中查找键
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
    }(blockingExecutor)
  }

  private[this] sealed trait Command
  private[this] final case class SetKey(key: Key, value: Value, callback: Value => Unit) extends Command
  private[this] final case class UpdateSegments(toAdd: List[SSTable], toRemove: List[SSTable]) extends Command
  private[this] final case class AddSegment(toAdd: SSTable, toRemove: MemoryTree) extends Command
  private[this] final case object PoisonPill extends Command

  private[this] val commandQueue: BlockingQueue[Command] = new LinkedBlockingQueue[Command]()

  /**
    * 设置键值对，若键已存在，新值将覆盖旧值
    *
    * @param key 待设置键
    * @param value 待设置值
    * @return 成功设置的值
    */
  override def set(key: Key, value: Value): Future[Value] = {
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
  private[this] val worker: Runnable = () => {
    val treeSizeThreshold = bufferSize
    var someoneKillMe = false

    while (!someoneKillMe) {
      val op: Command = commandQueue.take()

      op match {
        case SetKey(key, value, callback) => setKey(key, value, callback)

        case UpdateSegments(toAdd, toRemove) => updateSegments(toAdd, toRemove.map(_.id).toSet)

        case AddSegment(toAdd, toRemove) => addSegment(toAdd, toRemove)

        case PoisonPill => someoneKillMe = true
      }
    }

    /**
      * 先在内存树中存新键，待到内存树过大，则创建新内存树，旧内存树则转化成索引树
      *
      */
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

    /**
      * 内存树转化成索引树和文件
      *
      */
    def convertToSeg(memoryTree: MemoryTree): Future[Unit] = {
      // TODO: 稀疏
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

        val fileName = Files.write(storePath.resolve(s"$dbName-sst-$id"), bytes,
          StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).toString

        commandQueue.put(AddSegment(SSTable(id, fileName, indexTree), memoryTree))
      }(blockingExecutor)
    }

    /**
      * 内存树转化成索引树和文件后，更新状态
      *
      */
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

    /**
      * 清理旧的索引树，参数为压缩后的索引树和文件
      *
      */
    def updateSegments(toAdd: List[SSTable], toRemoveIds: Set[Int]): Unit = {
      val State(oldSegments, oldMemoryTrees) = state
      state = State(toAdd ++ oldSegments.filterNot(s => toRemoveIds.contains(s.id)), oldMemoryTrees)

      // TODO： 删除文件
      Future {

      }(blockingExecutor)
    }

  }
  val workerThread = new Thread(worker)
  workerThread.start()

  /**
    * 后台运行的压缩工作线程，定时执行
    */
  private[this] val compactWorker: Runnable = () => {
    if (state.segments.size <= 1) {
      commandQueue.put(UpdateSegments(List(), List()))
    } else {

    }
  }

  scheduledPool.scheduleWithFixedDelay(compactWorker, 10, 5, TimeUnit.SECONDS)

  def shutdown(): Unit = {
    commandQueue.clear()
    commandQueue.put(PoisonPill)
    blockingExecutor.shutdown()
    scheduledPool.shutdown()
  }

  private implicit lazy val globalExecutor: ExecutionContextExecutor = ExecutionContext.global
  private lazy val blockingExecutor: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
    Executors.newFixedThreadPool(20)
  )
  private lazy val scheduledPool: ScheduledExecutorService = Executors.newScheduledThreadPool(4)
}

