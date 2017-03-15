package com.columnix

import java.nio.file.{Files, Path}

import com.columnix.file.{FileReader, FileWriter, Filter}
import org.scalatest.{FlatSpec, Matchers}

trait Test extends FlatSpec with Matchers {

  def test[R](block: Path => R): R = {
    val file = Files.createTempFile("columnix", null)
    try block(file)
    finally Files.delete(file)
  }

  def withWriter[R](path: Path)(block: FileWriter => R): R = {
    val writer = new FileWriter(path.toString)
    try block(writer)
    finally writer.close()
  }

  def withReader[R](path: Path, filter: Option[Filter] = None)
                   (block: FileReader => R): R = {
    val reader = new FileReader(path.toString, filter)
    try block(reader)
    finally reader.close()
  }

  def empty(file: Path): Unit =
    withWriter(file)(_.finish())
}
