package com.columnix.jni

class Writer(path: String, rowGroupSize: Long = 1000000L)
  extends Serializable {

  System.loadLibrary("columnix")

  type Pointer = Long

  private[this] var ptr = cNew(path, rowGroupSize)

  def finish(sync: Boolean = false): Unit = cFinish(ptr, sync)

  def close(): Unit = {
    if (ptr == 0)
      return
    cFree(ptr)
    ptr = 0
  }

  def setMetadata(metadata: String): Unit = cMetadata(ptr, metadata)

  def addColumn(columnType: ColumnType.ColumnType,
                name: String,
                encoding: ColumnEncoding.ColumnEncoding = ColumnEncoding.None,
                compression: ColumnCompression.ColumnCompression = ColumnCompression.None,
                compressionLevel: Int = 0): Unit =
    cAddColumn(ptr, name, columnType.id, encoding.id, compression.id, compressionLevel)

  def putNull(index: Int): Unit = cPutNull(ptr, index)

  def putBoolean(index: Int, value: Boolean): Unit = cPutBoolean(ptr, index, value)

  def putInt(index: Int, value: Int): Unit = cPutInt(ptr, index, value)

  def putLong(index: Int, value: Long): Unit = cPutLong(ptr, index, value)

  def putString(index: Int, value: String): Unit = cPutString(ptr, index, value)

  @native private def cNew(path: String, rowGroupSize: Long): Pointer = ???

  @native private def cFree(writer: Pointer): Unit = ???

  @native private def cMetadata(writer: Pointer, metadata: String): Unit = ???

  @native private def cFinish(writer: Pointer, sync: Boolean): Unit = ???

  @native private def cAddColumn(writer: Pointer, name: String,
                                 columnType: Int, encoding: Int, compression: Int,
                                 compressionLevel: Int): Unit = ???

  @native private def cPutNull(writer: Pointer, index: Int): Unit = ???

  @native private def cPutBoolean(writer: Pointer, index: Int, value: Boolean): Unit = ???

  @native private def cPutInt(writer: Pointer, index: Int, value: Int): Unit = ???

  @native private def cPutLong(writer: Pointer, index: Int, value: Long): Unit = ???

  @native private def cPutString(writer: Pointer, index: Int, value: String): Unit = ???
}
