package com.gaufoo.sst

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import org.scalatest.{AsyncFlatSpec, FlatSpec, Matchers}
import org.slf4s.Logging

class SSTEngineTest extends AsyncFlatSpec with Logging with Matchers {
  private val dataFolder = Paths.get("resources")

  private def removeDbFolder(databaseName: String) : Unit = {
    if (dataFolder.resolve(databaseName).toFile.exists()) {
      Files.walkFileTree(dataFolder.resolve(databaseName), new SimpleFileVisitor[Path]{
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

  "initState" should "create a folder for the first time" in {
    val dbName = "testDB"
    removeDbFolder(dbName)
    SSTEngine.build(dbName, 20)
    val file = dataFolder.resolve(dbName).toFile
    file.exists() shouldBe true
    file should be a 'directory
    removeDbFolder(dbName)
    succeed
  }




}
