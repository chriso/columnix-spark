package zcs.jni

class Writer(path: String, rowGroupSize: Long = 1000000L)
  extends Serializable {

  System.loadLibrary("zcs")

  type Pointer = Long

  private[this] var ptr = nativeNew(path, rowGroupSize)

  def finish(sync: Boolean = false): Unit = nativeFinish(ptr, sync)

  def close(): Unit = {
    if (ptr == 0)
      return
    nativeFree(ptr)
    ptr = 0
  }

  def addColumn(`type`: ColumnType.ColumnType,
                name: String,
                encoding: ColumnEncoding.ColumnEncoding = ColumnEncoding.None,
                compression: ColumnCompression.ColumnCompression = ColumnCompression.None,
                compressionLevel: Int = 0): Unit =
    nativeAddColumn(ptr, name, `type`.id, encoding.id, compression.id, compressionLevel)

  def putNull(index: Int): Unit = nativePutNull(ptr, index)

  def putBoolean(index: Int, value: Boolean): Unit = nativePutBoolean(ptr, index, value)

  def putInt(index: Int, value: Int): Unit = nativePutInt(ptr, index, value)

  def putLong(index: Int, value: Long): Unit = nativePutLong(ptr, index, value)

  def putString(index: Int, value: String): Unit = nativePutString(ptr, index, value)

  @native private def nativeNew(path: String, rowGroupSize: Long): Pointer = ???

  @native private def nativeFree(writer: Pointer): Unit = ???

  @native private def nativeFinish(writer: Pointer, sync: Boolean): Unit = ???

  @native private def nativeAddColumn(writer: Pointer, name: String,
                                      `type`: Int, encoding: Int, compression: Int,
                                      compressionLevel: Int): Unit = ???

  @native private def nativePutNull(writer: Pointer, index: Int): Unit = ???

  @native private def nativePutBoolean(writer: Pointer, index: Int, value: Boolean): Unit = ???

  @native private def nativePutInt(writer: Pointer, index: Int, value: Int): Unit = ???

  @native private def nativePutLong(writer: Pointer, index: Int, value: Long): Unit = ???

  @native private def nativePutString(writer: Pointer, index: Int, value: String): Unit = ???
}
