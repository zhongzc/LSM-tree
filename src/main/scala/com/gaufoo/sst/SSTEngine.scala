package com.gaufoo.sst

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.collection.JavaConverters._
import java.util.concurrent._

import com.gaufoo.collection.immutable.TreeMap
import org.slf4s.LoggerFactory

import scala.collection.BitSet
import scala.concurrent._
import scala.concurrent.Future

object SSTEngine extends Types {
  def build(dbName: String, path: Path = null, bufferSize: Int = 1500): SSTEngine = {
    if (path == null)
      new SSTEngine(dbName, Paths.get("resources").resolve(dbName), bufferSize)
    else new SSTEngine(dbName, path, bufferSize)
  }

  final case class MemoryTree(id: Int, tree: TreeMap[Key, Value]) extends AnyRef {
    override def equals(obj: Any): Boolean = obj match {
      case that: AnyRef => this eq that
      case _ => false
    }
  }

  // indexTree 中的 value ： Boolean 代表值存在/已删除，true 代表存在
  final case class SSTable(idFrom: Int, idTo: Int, path: Path, indexTree: TreeMap[Key, (Boolean, Offset)]) {
    override def equals(obj: Any): Boolean = obj match {
      case that: AnyRef => this eq that
      case _ => false
    }

    lazy val compactRate: Int = idTo - idFrom
  }

  final case class State(segments: List[SSTable], immTrees: List[MemoryTree], curTree: MemoryTree)

  sealed trait Command

  final case class PutKey(key: Key, value: Value, callback: Value => Unit) extends Command

  final case class UpdateSegments(newSegments: List[SSTable], toRemove: List[SSTable]) extends Command

  final case class AddSegment(toAdd: SSTable, toRemove: MemoryTree) extends Command

  final case class PoisonPill(callback: () => Unit) extends Command
}

class SSTEngine(dbName: String, path: Path, bufferSize: Int) extends KVEngine {
  import SSTEngine._
  private[this] val _log = LoggerFactory.getLogger(this.getClass)
  private[this] var _isShutdown = false
  private[this] val storePath: Path = path

  // id generator
  private[this] var idFrom = 0
  private[this] lazy val genId: () => Int = {
    val inner = () => {
      idFrom = idFrom + 1
      idFrom - 1
    }
    inner
  }

  /**
    * 根据数据库文件夹初始化状态，若不存在则创建新数据库
    *
    * @param storePath 数据库文件路径
    * @return 根据快照创建好的状态
    */
  private[this] def initState(storePath: Path): State = {
    if (Files.notExists(storePath)) {
      Files.createDirectories(storePath)
      State(List(), List(), MemoryTree(genId(), TreeMap[Key, Value]()))
    } else {
      restoreSST(storePath)
    }
  }

  private[this] def restoreSST(path: Path): State = {
    var files = Files.list(path).iterator().asScala.toList.filter(f => !Files.isDirectory(f) && f.toString.contains("-"))

    def checkFiles(): Unit = {
      var bitset = BitSet()
      var tras = List[Path]()
      files.foreach { path =>
        val a = path.toString.split("-").last.split("_")
        val from = a.head.toInt
        val to = a.last.toInt
        if (bitset(from) && bitset(to)) {
          deleteFile(path)
        } else if (bitset(from) || bitset(to)) {
          val (toDel, rest) = tras.partition(p => {
            val h = p.toString.split("-").last.split("_").head.toInt
            from <= h && h <= to
          })
          toDel.foreach(deleteFile)
          tras = path :: rest
          bitset = bitset ++ (from to to)
        } else {
          tras = path :: tras
          bitset = bitset ++ (from to to)
        }
      }

      files = tras
    }

    checkFiles()

    def fileToSegment(path: Path): SSTable = {
      var indexTree = TreeMap[Key, (Boolean, Offset)]()
      var offset = 0
      val c: SeekableByteChannel = Files.newByteChannel(path, StandardOpenOption.READ)
      val intBuf = ByteBuffer.allocate(4)
      while (c.read(intBuf) > 0) {
        intBuf.flip()
        val keyLen = intBuf.getInt()
        intBuf.clear()
        val keyBuf = ByteBuffer.allocate(keyLen)
        c.read(keyBuf)

        val valueLen = readInt(intBuf, c).get
        val valueBuf = ByteBuffer.allocate(valueLen)
        c.read(valueBuf)

        val k = new String(keyBuf.array(), 0, keyLen)
        val v = new String(valueBuf.array(), 0, valueLen)

        offset = c.position.toInt - keyLen - valueLen - 8
        indexTree = indexTree.insert(k, (v.charAt(0) == 'v', offset))
      }
      c.close()

      val a = path.toString.split("-").last.split("_")
      val from = a.head.toInt
      val to = a.last.toInt
      SSTable(from, to, path, indexTree)
    }

    val segments = files.map(path => fileToSegment(path)).sortBy(-_.idFrom)
    idFrom = segments.headOption.map(_.idTo + 1).getOrElse(0)
    State(segments, List(), MemoryTree(genId(), TreeMap[Key, Value]()))
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
  override def get(key: Key): Future[Option[Value]] = doJob {
    val State(segments, immTrees, curTree) = state
    retrieveFromMemory(key, curTree :: immTrees).flatMap(value =>
      (if (value.isEmpty) {
        retrieveFromSegments(key, segments)
      } else {
        Future.successful(value)
      }).map(ov => ov.flatMap(v => if (v.charAt(0) == 'd') None else Option(v.substring(1))))
    )

  }

  /**
    * 删除某键，为了简单利用`tombstone`占位，压缩时才清除
    *
    * @param key 需要删除的键
    * @return 键原有的值
    */
  override def delete(key: Key): Future[Option[Value]] = doJob {
    for {
      ov <- get(key)
      _ <- put(key, "d")
    } yield ov
  }

  private[this] def retrieveFromMemory(key: Key, memoryTrees: List[MemoryTree]): Future[Option[Value]] = {
    doAsyncJob { memoryTrees.map(_.tree).find(_.contains(key)).map(_ (key)) }
  }

  private[this] def retrieveFromSegments(key: Key, segments: List[SSTable]): Future[Option[Value]] = {
    doAsyncJob {
      // 先从内存中的 indexTree 中查找键
      segments.find(s => s.indexTree.contains(key)).map {
        case SSTable(_, _, path, indexTree) =>
          if (!indexTree(key)._1) "d"
          else {
            var readChannel: SeekableByteChannel = null

            // 在这个时候，可能发生文件刚被删除的意外
            try {
              readChannel = Files.newByteChannel(path, StandardOpenOption.READ)
            } catch {
              case _: Exception =>
                readChannel = Files.newByteChannel(path.getParent.resolve("drop").resolve(path.getFileName), StandardOpenOption.READ)
            }

            readChannel.position(indexTree(key)._2)
            val intBuffer = ByteBuffer.allocate(4)

            val keyLen = readInt(intBuffer, readChannel).get
            val keyBuffer = ByteBuffer.allocate(keyLen)
            readChannel.read(keyBuffer)

            val valueLen = readInt(intBuffer, readChannel).get
            val valueBuffer = ByteBuffer.allocate(valueLen)
            readChannel.read(valueBuffer)

            readChannel.close()

            val k = new String(keyBuffer.array(), 0, keyLen)
            val v = new String(valueBuffer.array(), 0, valueLen)

            assert(k == key)
            v
          }
      }
    }
  }


  /**
    * 设置键值对，若键已存在，新值将覆盖旧值
    *
    * @param key   待设置键
    * @param value 待设置值
    * @return 成功设置的值
    */
  override def set(key: Key, value: Value): Future[Value] = doJob {
    put(key, new StringBuilder("v".length + value.length).append("v").append(value).toString)
      .map(_ => value)
  }

  private def put(key: Key, value: Value): Future[Value] = {
    val f = Promise[Value]()
    commandQueue.put(PutKey(key, value, { v =>f.success(v) }))
    f.future
  }


  /**
    * 唯一可以修改state的工作线程
    * 其他进程可以通过发送命令到命令队列上，此线程将按序处理命令
    *
    * 接受多种命令，包括：
    * `PutKey`: 设置键值对
    * `UpdateSegments`: 更新state中的段文件
    * `AddSegment`: 在内存转化成文件完成后，更新state中的段文件
    * `Compact`: 压缩SST
    *
    */
  private[this] lazy val commandQueue: BlockingQueue[Command] = new LinkedBlockingQueue[Command]()
  private[this] val worker: Runnable = () => {
    val treeSizeThreshold = bufferSize

    while (!_isShutdown) {
      commandQueue.take() match {
        case PutKey(key, value, callback) => putKey(key, value, callback)

        case UpdateSegments(newSegments, toRemove) => updateSegments(newSegments, toRemove)

        case AddSegment(toAdd, toRemove) => addSegment(toAdd, toRemove)

        case PoisonPill(callback) =>
          _log.info(s"SSTdb[$dbName] is shutting down...")
          _isShutdown = true
          futureExecutor.shutdown()
          scheduledPool.shutdown()
          futureExecutor.awaitTermination(Long.MaxValue, TimeUnit.MINUTES)
          scheduledPool.awaitTermination(Long.MaxValue, TimeUnit.MINUTES)

          val MemoryTree(id, tree) = state.curTree
          if (tree.size > 0) {
            val (bytes, _) = kvTreeToBytesAndIndexTree(tree)
            Files.write(storePath.resolve(s"$dbName-sst-${id}_to_$id"), bytes,
              StandardOpenOption.CREATE, StandardOpenOption.WRITE)
          }
          utils.Utils.removeDirIfExist(storePath.resolve("drop"))

          _log.info(s"SSTdb[$dbName] has been shut down.")
          callback()
      }
    }

    /**
      * 先在内存树中存新键，待到内存树过大，则创建新内存树，旧内存树则转化成索引树
      *
      */
    def putKey(key: Key, value: Value, callback: Value => Unit): Unit = {
      val State(oldSegments, immTrees, MemoryTree(id, oldTree)) = state
      val newTree = MemoryTree(id, oldTree.insert(key, value))

      if (newTree.tree.size >= treeSizeThreshold) {
        convertToSeg(newTree)
        state = State(oldSegments, newTree :: immTrees, MemoryTree(genId(), TreeMap[Key, Value]()))
      } else {
        state = State(oldSegments, immTrees, newTree)
      }

      callback(value)
    }

    /**
      * 内存树转化成索引树和文件
      *
      */
    def convertToSeg(memoryTree: MemoryTree): Future[Unit] = {
      doAsyncJob {
        val MemoryTree(id, tree) = memoryTree

        val (bytes, indexTree) = kvTreeToBytesAndIndexTree(tree)

        val path = Files.write(storePath.resolve(s"$dbName-sst-${id}_to_$id"), bytes,
          StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)

        commandQueue.put(AddSegment(SSTable(id, id, path, indexTree), memoryTree))
      }
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

        val State(oldSegments, immTrees, curTree) = state
        val idsToRemove = immTrees.map(_.id).reverse.takeWhile(blocking.isDefinedAt)
        val (ss, _) = idsToRemove.map(blocking).reverse.unzip
        state = State(ss ++ oldSegments, immTrees.filterNot(t => idsToRemove.contains(t.id)), curTree)
        blocking = blocking -- idsToRemove
      }

      inner
    }

    /**
      * 清理旧的索引树，参数为压缩后的索引树和文件
      *
      */
    def updateSegments(newSegments: List[SSTable], toRemove: List[SSTable]): Unit = {
      val State(oldSegments, immTrees, curTree) = state
      val moreLen = oldSegments.length - toRemove.length + 1 - newSegments.length
      val realNew = oldSegments.take(moreLen) ++ newSegments

      state = State(realNew, immTrees, curTree)
      if (!scheduledPool.isShutdown) scheduledPool.schedule(compactWorker, 10000 / realNew.length, TimeUnit.MILLISECONDS)
      else _log.warn(s"scheduledPool has been shut down.")

      doAsyncJob {
        toRemove.map(_.path).foreach(p => {
          deleteFile(p)
        })
      }
    }

  }
  private[this] val workerThread = new Thread(worker)
  workerThread.start()

  /**
    * 后台运行的压缩工作线程，定时执行
    */
  private[this] var compactRateThreshold = 4
  private[this] val compactWorker: Runnable = () => {
    try {
      val State(segments, _, _) = state
      val (rest, fresh) = segments.reverse.span(_.compactRate >= compactRateThreshold)
      fresh.take(4) match {
        case l@List(_, _, _, _) =>
          var tree = TreeMap[Key, Value]()
          l.foreach {
            s =>
              val c = Files.newByteChannel(s.path, StandardOpenOption.READ)
              val intBuf = ByteBuffer.allocate(4)
              while (c.read(intBuf) > 0) {
                intBuf.flip()
                val keyLen = intBuf.getInt()
                intBuf.clear()
                val keyBuf = ByteBuffer.allocate(keyLen)
                c.read(keyBuf)

                val valueLen = readInt(intBuf, c).get
                val valueBuf = ByteBuffer.allocate(valueLen)
                c.read(valueBuf)

                val k = new String(keyBuf.array(), 0, keyLen)
                val v = new String(valueBuf.array(), 0, valueLen)

                tree = tree.insert(k, v)
              }
              c.close()
          }

          val idFrom: Int = l.head.idFrom
          val idTo: Int = l.last.idTo
          val (bytes, indexTree) = kvTreeToBytesAndIndexTree(tree, idFrom == 0)
          val path = Files.write(storePath.resolve(s"$dbName-sst-${
            idFrom + "_to_" + idTo
          }"), bytes,
            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)

          val (rest, right) = segments.span(!l.contains(_))
          val (rms, last) = right.span(l.contains(_))
          commandQueue.put(UpdateSegments(rest ++ (SSTable(idFrom, idTo, path, indexTree) :: last), rms))
        case _ =>
          if (rest.length > 4) {
            compactRateThreshold = math.min(30, compactRateThreshold + 1)
//            _log.info(s"compact rate: $compactRateThreshold")
          }
          if (!scheduledPool.isShutdown)
            scheduledPool.schedule(compactWorker, 10000 / math.max(1, segments.size), TimeUnit.MILLISECONDS)
          else _log.warn(s"scheduledPool has been shut down.")
      }
    } catch {
      case e: Throwable =>
        _log.warn(s"${e.toString}")
        if (!scheduledPool.isShutdown) scheduledPool.schedule(compactWorker, 5, TimeUnit.SECONDS)
        else _log.warn(s"scheduledPool has been shutdown.")
    }
  }

  scheduledPool.schedule(compactWorker, 5, TimeUnit.SECONDS)

  override def shutdown(): Future[Boolean] = doJob {
    val p = Promise[Boolean]()
    commandQueue.put(PoisonPill(() => { p.success(true) }))
    p.future
  }

  /**
    * Future 执行环境、以及后台压缩执行环境
    */
  private implicit lazy val futureExecutor: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
    Executors.newFixedThreadPool(20)
  )
  private lazy val scheduledPool: ScheduledExecutorService = Executors.newScheduledThreadPool(4)


  override def allKeysAsc(): Future[List[Key]] = doJob {
    doAsyncJob {
      val State(segments, immTrees, curTree) = state
      var treeMap = TreeMap.empty[Key, Unit]
      segments.reverse.foreach { case SSTable(_, _, _, indexTree) =>
        indexTree.foreach { case (k, (b, _)) => treeMap = if (b) treeMap.insert(k, Unit) else treeMap.remove(k) }
      }
      immTrees.reverse.foreach { case MemoryTree(_, tree) =>
        tree.foreach { case (k, v) => treeMap = if (v.charAt(0) == 'v') treeMap.insert(k, Unit) else treeMap.remove(k) }
      }
      curTree.tree.foreach { case (k, v) => treeMap = if (v.charAt(0) == 'v') treeMap.insert(k, Unit) else treeMap.remove(k) }

      treeMap.mapAsc{ case (k, _) => k }
    }
  }

  override def allKeysDes(): Future[List[Key]] = doJob {
    doAsyncJob {
      val State(segments, immTrees, curTree) = state
      var treeMap = TreeMap.empty[Key, Unit]
      segments.reverse.foreach { case SSTable(_, _, _, indexTree) =>
        indexTree.foreach { case (k, (b, _)) => treeMap = if (b) treeMap.insert(k, Unit) else treeMap.remove(k) }
      }
      immTrees.reverse.foreach { case MemoryTree(_, tree) =>
        tree.foreach { case (k, v) => treeMap = if (v.charAt(0) == 'v') treeMap.insert(k, Unit) else treeMap.remove(k) }
      }
      curTree.tree.foreach { case (k, v) => treeMap = if (v.charAt(0) == 'v') treeMap.insert(k, Unit) else treeMap.remove(k) }

      treeMap.mapDes{ case (k, _) => k }
    }
  }

  override def rangeKeysAsc(left: Key, right: Key): Future[List[Key]] = {
    assert(left <= right)
    doAsyncJob {
      val State(segments, immTrees, curTree) = state
      var treeMap = TreeMap.empty[Key, Unit]
      segments.reverse.foreach { case SSTable(_, _, _, indexTree) =>
        indexTree.mapInRangeAsc(left, right, { case (k, (b, _)) => treeMap = if (b) treeMap.insert(k, Unit) else treeMap.remove(k) })
      }
      immTrees.reverse.foreach { case MemoryTree(_, tree) =>
        tree.mapInRangeAsc(left, right, { case (k, v) => treeMap = if (v.charAt(0) == 'v') treeMap.insert(k, Unit) else treeMap.remove(k) })
      }
      curTree.tree.mapInRangeAsc(left, right, { case (k, v) => treeMap = if (v.charAt(0) == 'v') treeMap.insert(k, Unit) else treeMap.remove(k) })

      treeMap.mapAsc{ case (k, _) => k }
    }
  }

  override def rangeKeysDes(right: Key, left: Key): Future[List[Key]] = {
    assert(left <= right)
    doAsyncJob {
      val State(segments, immTrees, curTree) = state
      var treeMap = TreeMap.empty[Key, Unit]
      segments.reverse.foreach { case SSTable(_, _, _, indexTree) =>
        indexTree.mapInRangeAsc(left, right, { case (k, (b, _)) => treeMap = if (b) treeMap.insert(k, Unit) else treeMap.remove(k) })
      }
      immTrees.reverse.foreach { case MemoryTree(_, tree) =>
        tree.mapInRangeAsc(left, right, { case (k, v) => treeMap = if (v.charAt(0) == 'v') treeMap.insert(k, Unit) else treeMap.remove(k) })
      }
      curTree.tree.mapInRangeAsc(left, right, { case (k, v) => treeMap = if (v.charAt(0) == 'v') treeMap.insert(k, Unit) else treeMap.remove(k) })

      treeMap.mapDes{ case (k, _) => k }
    }
  }

  override def isShutdown: Boolean = _isShutdown

  private def doJob[T](job: => T): T =
    if (_isShutdown) throw new Exception(s"SSTdb[$dbName] has been shutdown")
    else job

  private def doAsyncJob[T](asyncJob: => T)(implicit executor: ExecutionContext): Future[T] =
    if (futureExecutor.isShutdown) Future.successful(asyncJob)
    else Future(asyncJob)(executor)

  private def kvTreeToBytesAndIndexTree(tree: TreeMap[Key, Value], dropDel: Boolean = false): (Array[Byte], TreeMap[Key, (Boolean, Offset)]) = {
    val intLength = 4
    val (listOfArray, _, indexTree) = tree.foldLeft((Vector.empty[Array[Byte]], 0, TreeMap.empty[Key, (Boolean, Offset)])) {
      case ((acc, offset, iTree), (key, value)) =>
        val bfk = ByteBuffer.allocate(intLength)
        val bfv = ByteBuffer.allocate(intLength)

        val keyBytes = key.getBytes(StandardCharsets.UTF_8)
        val valueBytes = value.getBytes(StandardCharsets.UTF_8)
        if (!dropDel || value.charAt(0) == 'v') {
          val accOffset = intLength + keyBytes.length + intLength + valueBytes.length + offset
          (acc :+ bfk.putInt(keyBytes.length).array() :+ keyBytes :+
            bfv.putInt(valueBytes.length).array() :+ valueBytes,
            accOffset,
            iTree.insert(key, (value.charAt(0) == 'v', offset)))
        } else {
          (acc, offset, iTree)
        }
    }
    val bytes = listOfArray.toArray.flatten
    (bytes, indexTree)
  }

  private def deleteFile(path: Path): Unit = {
    val parent = path.getParent
    val newName = parent.resolve("drop").resolve(path.getFileName)
    if (Files.notExists(newName.getParent)) Files.createDirectory(newName.getParent)
    Files.move(path, newName)
  }

  private def readInt(buf: ByteBuffer, channel: SeekableByteChannel): Option[Int] = {
    if (channel.read(buf) > 0) {
      buf.flip()
      val i = buf.getInt
      buf.clear()
      Option(i)
    } else None
  }

}
