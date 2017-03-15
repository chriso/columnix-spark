package com.columnix

import java.nio.file.{Files, Path}

import com.columnix.jni.{Filter, NativeReader, NativeWriter}
import org.scalatest.{FlatSpec, Matchers}

trait Test extends FlatSpec with Matchers {

  def test[R](block: Path => R): R = {
    val file = Files.createTempFile("columnix", null)
    try block(file)
    finally Files.delete(file)
  }

  def withWriter[R](path: Path)(block: NativeWriter => R): R = {
    val writer = new NativeWriter(path.toString)
    try block(writer)
    finally writer.close()
  }

  def withReader[R](path: Path, filter: Option[Filter] = None)
                   (block: NativeReader => R): R = {
    val reader = new NativeReader(path.toString, filter)
    try block(reader)
    finally reader.close()
  }

  def empty(file: Path): Unit =
    withWriter(file)(_.finish())
}
