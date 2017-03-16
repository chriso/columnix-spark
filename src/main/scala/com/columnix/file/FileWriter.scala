package com.columnix.file

import com.columnix.jni._

class FileWriter(path: String, rowGroupSize: Long = 1000000L)
  extends Serializable {

  require(rowGroupSize > 0L, "invalid row group size")

  private[this] val native = new Writer

  private[this] var ptr = native.create(path, rowGroupSize)

  private[this] var columnCount: Int = 0

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

  def finish(sync: Boolean = false): Unit = {
    checkPointer()
    native.finish(ptr, sync)
  }

  def setMetadata(metadata: String): Unit = {
    checkPointer()
    native.setMetadata(ptr, metadata)
  }

  def addColumn(columnType: ColumnType.ColumnType,
                name: String,
                encoding: ColumnEncoding.ColumnEncoding = ColumnEncoding.None,
                compression: ColumnCompression.ColumnCompression = ColumnCompression.None,
                compressionLevel: Int = 0): Unit = {
    checkPointer()
    native.addColumn(ptr, name, columnType.id, encoding.id, compression.id, compressionLevel)
    columnCount += 1
  }

  def putNull(column: Int): Unit = {
    checkBounds(column)
    native.putNull(ptr, column)
  }

  def putBoolean(column: Int, value: Boolean): Unit = {
    checkBounds(column)
    native.putBoolean(ptr, column, value)
  }

  def putInt(column: Int, value: Int): Unit = {
    checkBounds(column)
    native.putInt(ptr, column, value)
  }

  def putLong(column: Int, value: Long): Unit = {
    checkBounds(column)
    native.putLong(ptr, column, value)
  }

  def putFloat(column: Int, value: Float): Unit = {
    checkBounds(column)
    native.putFloat(ptr, column, value)
  }

  def putDouble(column: Int, value: Double): Unit = {
    checkBounds(column)
    native.putDouble(ptr, column, value)
  }

  def putString(column: Int, value: String): Unit = {
    checkBounds(column)
    native.putString(ptr, column, value)
  }
}
