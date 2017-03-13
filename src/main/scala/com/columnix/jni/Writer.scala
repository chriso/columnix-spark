package com.columnix.jni

class Writer(path: String, rowGroupSize: Long = 1000000L)
  extends Serializable {

  private[this] val native = new c.Writer

  private[this] var ptr = native.create(path, rowGroupSize)

  def finish(sync: Boolean = false): Unit =
    native.finish(ptr, sync)

  def close(): Unit = {
    if (ptr == 0L)
      return
    native.free(ptr)
    ptr = 0L
  }

  def setMetadata(metadata: String): Unit =
    native.setMetadata(ptr, metadata)

  def addColumn(columnType: ColumnType.ColumnType,
                name: String,
                encoding: ColumnEncoding.ColumnEncoding = ColumnEncoding.None,
                compression: ColumnCompression.ColumnCompression = ColumnCompression.None,
                compressionLevel: Int = 0): Unit =
    native.addColumn(ptr, name, columnType.id, encoding.id, compression.id, compressionLevel)

  def putNull(index: Int): Unit =
    native.putNull(ptr, index)

  def putBoolean(index: Int, value: Boolean): Unit =
    native.putBoolean(ptr, index, value)

  def putInt(index: Int, value: Int): Unit =
    native.putInt(ptr, index, value)

  def putLong(index: Int, value: Long): Unit =
    native.putLong(ptr, index, value)

  def putString(index: Int, value: String): Unit =
    native.putString(ptr, index, value)
}
