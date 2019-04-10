package com.gaufoo.sst.utils

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}


private[sst] object Utils {
  def removeDirIfExist(path: Path): Unit = {
    if (path.toFile.exists()) {
      Files.walkFileTree(path, new SimpleFileVisitor[Path] {
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