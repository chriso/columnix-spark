package com.columnix.jni

class Reader(path: String, filter: Option[Filter] = None) {

  System.loadLibrary("columnix")

  type Pointer = Long

  private[this] var ptr = filter match {
    case None => cNew(path)
    case Some(f) => cNewMatching(path, Predicate.fromFilter(f))
  }

  def close() {
    if (ptr == 0)
      return
    cFree(ptr)
    ptr = 0
  }

  def columnCount: Int = cColumnCount(ptr)

  def rowCount: Long = cRowCount(ptr)

  def rewind(): Unit = cRewind(ptr)

  def next: Boolean = cNext(ptr)

  def metadata: Option[String] = Option(cMetadata(ptr))

  def columnName(column: Int): String = cColumnName(ptr, column)

  def columnType(column: Int): ColumnType.ColumnType =
    ColumnType(cColumnType(ptr, column))

  def columnEncoding(column: Int): ColumnEncoding.ColumnEncoding =
    ColumnEncoding(cColumnEncoding(ptr, column))

  def columnCompression(column: Int): ColumnCompression.ColumnCompression =
    ColumnCompression(cColumnCompression(ptr, column))

  def isNull(column: Int): Boolean = cIsNull(ptr, column)

  def getBoolean(column: Int): Boolean = cGetBoolean(ptr, column)

  def getInt(column: Int): Int = cGetInt(ptr, column)

  def getLong(column: Int): Long = cGetLong(ptr, column)

  def getString(column: Int): String = cGetString(ptr, column)

  def getStringBytes(column: Int): Array[Byte] = cGetStringBytes(ptr, column)

  @native private def cNew(path: String): Long = ???

  @native private def cNewMatching(path: String, predicate: Predicate.Pointer): Pointer = ???

  @native private def cFree(reader: Pointer): Unit = ???

  @native private def cMetadata(reader: Pointer): String = ???

  @native private def cColumnCount(reader: Pointer): Int = ???

  @native private def cRowCount(reader: Pointer): Long = ???

  @native private def cRewind(reader: Pointer): Unit = ???

  @native private def cNext(reader: Pointer): Boolean = ???

  @native private def cColumnName(reader: Pointer, index: Int): String = ???

  @native private def cColumnType(reader: Pointer, index: Int): Int = ???

  @native private def cColumnEncoding(reader: Pointer, index: Int): Int = ???

  @native private def cColumnCompression(reader: Pointer, index: Int): Int = ???

  @native private def cIsNull(reader: Pointer, index: Int): Boolean = ???

  @native private def cGetBoolean(reader: Pointer, index: Int): Boolean = ???

  @native private def cGetInt(reader: Pointer, index: Int): Int = ???

  @native private def cGetLong(reader: Pointer, index: Int): Long = ???

  @native private def cGetString(reader: Pointer, index: Int): String = ???

  @native private def cGetStringBytes(reader: Pointer, index: Int): Array[Byte] = ???
}