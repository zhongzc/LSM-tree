package com.gaufoo

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.gaufoo.BasicAsyncFlatSpec.removeDbFolderIfExist
import com.gaufoo.sst.{KVEngine, SSTEngine}
import org.scalatest.{Assertion, AsyncFlatSpec, Matchers}
import org.slf4s.Logging

import scala.collection.mutable
import scala.util.Random

class BasicAsyncFlatSpec extends AsyncFlatSpec with Matchers with Logging {
  val dbName = "testDB"
  val dbLocation = s"resources/$dbName"

  def withTestEngine(bufferSize: Int)(testCode: KVEngine => Any): Assertion = {
    removeDbFolderIfExist(dbLocation)
    val engine = SSTEngine.build(dbName, bufferSize)

    testCode(engine)

    engine.shutdown()
    removeDbFolderIfExist(dbLocation)
    succeed
  }

}

object BasicAsyncFlatSpec {
  def randomString(maxLength: Int): String = {
    val l = Random.nextInt(maxLength + 1)
    val result = new mutable.StringBuilder()
    for (_ <- 1 to l) {
      result.append(Random.nextPrintableChar())
    }
    result.toString()
  }

  def removeDbFolderIfExist(dataFolderName: String) : Unit = {
    val path = Paths.get(dataFolderName)
    if (path.toFile.exists()) {
      Files.walkFileTree(path, new SimpleFileVisitor[Path]{
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }
        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Option(exc) match {
            case None => Files.delete(dir)
              FileVisitResult.CONTINUE
            case _ => throw new IOException
          }
        }
      })
    }
  }
}