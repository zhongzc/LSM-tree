package com.gaufoo.sst

import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.concurrent._

import com.gaufoo.collection.immutable.TreeMap
import org.slf4s.LoggerFactory

import scala.concurrent._
import scala.concurrent.Future

object SSTEngine extends Types {
  private val UTF_8 = "UTF-8"

  def build(dbName: String, bufferSize: Int = 1500): SSTEngine = {
    new SSTEngine(dbName, bufferSize)
  }

  final case class MemoryTree(id: Int, tree: TreeMap[Key, Value]) extends AnyRef {
    override def equals(obj: Any): Boolean = obj match {
      case that: AnyRef => this eq that
      case _ => false
    }
  }

  final case class SSTable(ids: List[Int], path: Path, indexTree: TreeMap[Key, Offset]) {
    override def equals(obj: Any): Boolean = obj match {
      case that: AnyRef => this eq that
      case _ => false
    }
  }

  final case class State(segments: List[SSTable], immTrees: List[MemoryTree], curTree: MemoryTree)

  sealed trait Command

  final case class PutKey(key: Key, value: Value, callback: Value => Unit) extends Command

  final case class UpdateSegments(toAdd: SSTable, toRemove: List[SSTable]) extends Command

  final case class AddSegment(toAdd: SSTable, toRemove: MemoryTree) extends Command

  final case object PoisonPill extends Command

  private def readByteBuffer(buffer: ByteBuffer): String = {
    val length = buffer.getInt()
    val ret = new String(buffer.array(), buffer.position(), length)
    buffer.position(buffer.position() + length)
    ret
  }

  private def kvTreeToBytesAndIndexTree(tree: TreeMap[Key, Value], dropDel: Boolean = false): (Array[Byte], TreeMap[Key, Offset]) = {
    val intLength = 4
    val (listOfArray, _, indexTree) = tree.foldLeft((Vector.empty[Array[Byte]], 0, TreeMap.empty[Key, Offset])) {
      case ((acc, offset, iTree), (key, value)) =>
        val bfk = ByteBuffer.allocate(intLength)
        val bfv = ByteBuffer.allocate(intLength)

        val keyBytes = key.getBytes(UTF_8)
        val valueBytes = value.getBytes(UTF_8)
        if (!dropDel || value.charAt(0) == 'v') {
          val accOffset = intLength + keyBytes.length + intLength + valueBytes.length + offset
          (acc :+ bfk.putInt(keyBytes.length).array() :+ keyBytes :+
            bfv.putInt(valueBytes.length).array() :+ valueBytes,
            accOffset,
            iTree.insert(key, offset))
        } else {
          (acc, offset, iTree)
        }
    }
    val bytes = listOfArray.toArray.flatten
    (bytes, indexTree)
  }
}

class SSTEngine(dbName: String, bufferSize: Int) extends KVEngine {
  val log = LoggerFactory.getLogger(this.getClass)

  import SSTEngine._

  private[this] val storePath: Path = Paths.get(s"resources/$dbName")

  private[this] lazy val genId: () => Int = {
    var id = -1
    val inner = () => {
      id = id + 1
      id
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

      // TODO: 根据文件夹重新构建索引树
      State(List(), List(), MemoryTree(genId(), TreeMap[Key, Value]()))
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
  override def delete(key: Key): Future[Option[Value]] = {
    for {
      ov <- get(key)
      _ <- put(key, "d")
    } yield ov
  }

  private[this] def retrieveFromMemory(key: Key, memoryTrees: List[MemoryTree]): Future[Option[Value]] = {
    Future {
      memoryTrees.map(_.tree).find(_.contains(key)).map(_ (key))
    }
  }

  private[this] def retrieveFromSegments(key: Key, segments: List[SSTable]): Future[Option[Value]] = {
    val intLength = 4
    Future {
      // 先从内存中的索引树中查找键
      segments.find(_.indexTree.contains(key)).map {
        case SSTable(_, path, indexTree) =>
          val readChannel = Files.newByteChannel(path, StandardOpenOption.READ)
          readChannel.position(indexTree(key))
          val intBuffer = ByteBuffer.allocate(intLength)
          readChannel.read(intBuffer)

          intBuffer.flip()
          val keyLen = intBuffer.getInt()
          val keyBuffer = ByteBuffer.allocate(keyLen)
          readChannel.read(keyBuffer)

          intBuffer.clear()
          readChannel.read(intBuffer)

          intBuffer.flip()
          val valueLen = intBuffer.getInt()
          val valueBuffer = ByteBuffer.allocate(valueLen)
          readChannel.read(valueBuffer)

          readChannel.close()

          val k = new String(keyBuffer.array(), 0, keyLen)
          val v = new String(valueBuffer.array(), 0, valueLen)

          assert(k == key)
          v
      }
    }(if (blockingExecutor.isShutdown) globalExecutor else blockingExecutor)
  }


  /**
    * 设置键值对，若键已存在，新值将覆盖旧值
    *
    * @param key   待设置键
    * @param value 待设置值
    * @return 成功设置的值
    */
  override def set(key: Key, value: Value): Future[Value] = {
    put(key, new StringBuilder("v".length + value.length).append("v").append(value).toString)
      .map(_ => value)
  }

  private def put(key: Key, value: Value): Future[Value] = {
    val f = Promise[Value]()

    commandQueue.put(PutKey(key, value, { v =>
      f.success(v)
    }))

    f.future
  }

  lazy val commandQueue: BlockingQueue[Command] = new LinkedBlockingQueue[Command]()
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
  private[this] val worker: Runnable = () => {
    val treeSizeThreshold = bufferSize
    var someoneKillMe = false

    while (!someoneKillMe) {
      val op: Command = commandQueue.take()

      op match {
        case PutKey(key, value, callback) => putKey(key, value, callback)

        case UpdateSegments(toAdd, toRemove) => updateSegments(toAdd, toRemove)

        case AddSegment(toAdd, toRemove) => addSegment(toAdd, toRemove)

        case PoisonPill =>
          someoneKillMe = true
          blockingExecutor.shutdown()
          scheduledPool.shutdown()
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
      Future {
        val MemoryTree(id, tree) = memoryTree

        val (bytes, indexTree) = kvTreeToBytesAndIndexTree(tree)

        val path = Files.write(storePath.resolve(s"$dbName-sst-$id"), bytes,
          StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)

        commandQueue.put(AddSegment(SSTable(List(id), path, indexTree), memoryTree))
      }(if (blockingExecutor.isShutdown) globalExecutor else blockingExecutor)
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
    def updateSegments(toAdd: SSTable, toRemove: List[SSTable]): Unit = {

      val State(oldSegments, immTrees, curTree) = state
      val (rest, right) = oldSegments.span(!toRemove.contains(_))
      val (rms, last) = right.span(toRemove.contains(_))
      //      log.debug(s"updateSegments: $toAdd $toRemove")
      //      log.debug(s"updateSegments: ${rms.reverse} == $toRemove ? ${rms.reverse == toRemove}")
      //      log.debug(s"updateSegments: rms = $rms, rest = $rest ? ${rms.reverse == toRemove}")
      if (rms == toRemove) {
        //        log.debug(s"updateSegments: ${rest.map(_.ids)}, ${rms.map(_.ids)}, ${last.map(_.ids)}")
        state = State(rest ++ (toAdd :: last), immTrees, curTree)

        Future {
          //          log.debug(s"updateSegments Future: in future")
          toRemove.map(_.path).foreach(p => {
            val parent = p.getParent
            val newName = parent.resolve("drop").resolve(p.getFileName)
            //            log.debug(s"updateSegments Future: $newName")
            if (Files.notExists(newName.getParent)) Files.createDirectory(newName.getParent)
            Files.move(p, newName)
          })
        }(if (blockingExecutor.isShutdown) globalExecutor else blockingExecutor)
      }
    }

  }
  private[this] val workerThread = new Thread(worker)
  workerThread.start()

  /**
    * 后台运行的压缩工作线程，定时执行
    */
  private[this] var compactRate = 4
  private[this] val compactWorker: Runnable = () => {
    val State(segments, _, _) = state
    //    log.debug(s"compactWorker: ${segments.map(_.ids)}")
    val len = segments.length
    val (rest, flash) = segments.reverse.span(_.ids.length >= compactRate)
    flash.take(4) match {
      case l@List(_, _, _, _) =>
        var tree = TreeMap[Key, Value]()
        l.map(s => ByteBuffer.wrap(Files.readAllBytes(s.path))).foreach { buf =>
          while (buf.hasRemaining) {
            val k = readByteBuffer(buf)
            val v = readByteBuffer(buf)
            tree = tree.insert(k, v)
          }
        }
        val (bytes, indexTree) = kvTreeToBytesAndIndexTree(tree, dropDel = true)
        val ids: List[Int] = l.flatMap(_.ids)
        val path = Files.write(storePath.resolve(s"$dbName-sst-${ids.mkString("_")}"), bytes,
          StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)

        commandQueue.put(UpdateSegments(SSTable(ids, path, indexTree), l.reverse))
      case _ =>
        if (rest.length >= len * .8 && len > 4) {
          compactRate += 2
          //          log.debug(s"compact rate: $compactRate")
        }
    }

  }


  scheduledPool.scheduleWithFixedDelay(compactWorker, 5, 3, TimeUnit.SECONDS)

  override def shutdown(): Unit = {
    commandQueue.put(PoisonPill)
  }

  /**
    * 两种Future执行环境
    * 一种为默认，用于非阻塞，和CPU核数匹配
    * 一种用于阻塞操作，如I/O
    *
    * 另外还有定时任务执行环境，供后台压缩使用
    */
  private implicit lazy val globalExecutor: ExecutionContextExecutor = ExecutionContext.global
  private lazy val blockingExecutor: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
    Executors.newFixedThreadPool(20)
  )
  private lazy val scheduledPool: ScheduledExecutorService = Executors.newScheduledThreadPool(4)
}
