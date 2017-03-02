package zcs.jni

import java.nio.file.{Files, Path}

import org.scalatest.{FlatSpec, Matchers}

trait Test extends FlatSpec with Matchers {

  def withTemporaryFile[R](block: Path => R): R = {
    val path = Files.createTempFile("zcs", null)
    try block(path)
    finally Files.delete(path)
  }

  def withWriter[R](path: Path)(block: Writer => R): R = {
    val writer = new Writer(path.toString)
    try block(writer)
    finally writer.close()
  }

  def withReader[R](path: Path)(block: Reader => R): R = {
    val reader = new Reader(path.toString)
    try block(reader)
    finally reader.close()
  }
}
