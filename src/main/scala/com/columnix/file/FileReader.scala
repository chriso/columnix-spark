package com.columnix.file

import com.columnix.jni._

class FileReader(path: String, filter: Option[Filter] = None) {

  private[this] val native = new Reader

  private[this] var ptr = filter match {
    case None => native.create(path)
    case Some(f) =>
      val predicate = PredicateTranslator.fromFilter(f)
      if (predicate == 0L)
        throw new NullPointerException
      native.createMatching(path, predicate)
  }

  val columnCount: Int = {
    checkPointer()
    native.columnCount(ptr)
  }

  private def checkPointer() =
    if (ptr == 0L)
      throw new NullPointerException

  private def checkBounds(column: Int) = {
    checkPointer()
    if (column < 0 || column >= columnCount)
      throw new IndexOutOfBoundsException
  }

  def close(): Unit = {
    if (ptr != 0L)
      native.free(ptr)
    ptr = 0L
  }

  def rowCount: Long = {
    checkPointer()
    native.rowCount(ptr)
  }

  def rewind(): Unit = {
    checkPointer()
    native.rewind(ptr)
  }

  def next: Boolean = {
    checkPointer()
    native.next(ptr)
  }

  def metadata: Option[String] = {
    checkPointer()
    Option(native.getMetadata(ptr))
  }

  def columnName(column: Int): String = {
    checkBounds(column)
    native.columnName(ptr, column)
  }

  def columnType(column: Int): ColumnType.ColumnType = {
    checkBounds(column)
    ColumnType(native.columnType(ptr, column))
  }

  def columnEncoding(column: Int): ColumnEncoding.ColumnEncoding = {
    checkBounds(column)
    ColumnEncoding(native.columnEncoding(ptr, column))
  }

  def columnCompression(column: Int): ColumnCompression.ColumnCompression = {
    checkBounds(column)
    ColumnCompression(native.columnCompression(ptr, column))
  }

  def isNull(column: Int): Boolean = {
    checkBounds(column)
    native.isNull(ptr, column)
  }

  def getBoolean(column: Int): Boolean = {
    checkBounds(column)
    native.getBoolean(ptr, column)
  }

  def getInt(column: Int): Int = {
    checkBounds(column)
    native.getInt(ptr, column)
  }

  def getLong(column: Int): Long = {
    checkBounds(column)
    native.getLong(ptr, column)
  }

  def getFloat(column: Int): Float = {
    checkBounds(column)
    native.getFloat(ptr, column)
  }

  def getDouble(column: Int): Double = {
    checkBounds(column)
    native.getDouble(ptr, column)
  }

  def getString(column: Int): String = {
    checkBounds(column)
    native.getString(ptr, column)
  }

  def getStringBytes(column: Int): Array[Byte] = {
    checkBounds(column)
    native.getStringBytes(ptr, column)
  }
}