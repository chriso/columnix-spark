package com.columnix.jni

class Reader(path: String, filter: Option[Filter] = None) {

  private[this] val native = new c.Reader

  private[this] var ptr = filter match {
    case None => native.create(path)
    case Some(f) => native.createMatching(path, Predicate.fromFilter(f))
  }

  def close(): Unit = {
    if (ptr == 0L)
      return
    native.free(ptr)
    ptr = 0L
  }

  def columnCount: Int =
    native.columnCount(ptr)

  def rowCount: Long =
    native.rowCount(ptr)

  def rewind(): Unit =
    native.rewind(ptr)

  def next: Boolean =
    native.next(ptr)

  def metadata: Option[String] =
    Option(native.getMetadata(ptr))

  def columnName(column: Int): String =
    native.columnName(ptr, column)

  def columnType(column: Int): ColumnType.ColumnType =
    ColumnType(native.columnType(ptr, column))

  def columnEncoding(column: Int): ColumnEncoding.ColumnEncoding =
    ColumnEncoding(native.columnEncoding(ptr, column))

  def columnCompression(column: Int): ColumnCompression.ColumnCompression =
    ColumnCompression(native.columnCompression(ptr, column))

  def isNull(column: Int): Boolean =
    native.isNull(ptr, column)

  def getBoolean(column: Int): Boolean =
    native.getBoolean(ptr, column)

  def getInt(column: Int): Int =
    native.getInt(ptr, column)

  def getLong(column: Int): Long =
    native.getLong(ptr, column)

  def getString(column: Int): String =
    native.getString(ptr, column)

  def getStringBytes(column: Int): Array[Byte] =
    native.getStringBytes(ptr, column)
}