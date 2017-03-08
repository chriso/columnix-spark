package zcs.jni

import java.nio.file.{Files, Path}

import org.scalatest.{FlatSpec, Matchers}

trait Test extends FlatSpec with Matchers {

  def test[R](block: Path => R): R = {
    val file = Files.createTempFile("zcs", null)
    try block(file)
    finally Files.delete(file)
  }

  def withWriter[R](path: Path)(block: Writer => R): R = {
    val writer = new Writer(path.toString)
    try block(writer)
    finally writer.close()
  }

  def withReader[R](path: Path, filter: Option[Filter] = None)
                   (block: Reader => R): R = {
    val reader = new Reader(path.toString, filter)
    try block(reader)
    finally reader.close()
  }
}
