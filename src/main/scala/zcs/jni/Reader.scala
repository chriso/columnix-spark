package zcs.jni

class Reader(path: String, filter: Option[Filter] = None) {

  System.loadLibrary("zcs")

  type Pointer = Long

  private[this] var ptr = filter match {
    case None => nativeNew(path)
    case Some(f) => nativeNewMatching(path, Predicate.fromFilter(f))
  }

  def close() {
    if (ptr == 0)
      return
    nativeFree(ptr)
    ptr = 0
  }

  def columnCount: Int = nativeColumnCount(ptr)

  def rowCount: Long = nativeRowCount(ptr)

  def rewind(): Unit = nativeRewind(ptr)

  def next: Boolean = nativeNext(ptr)

  def columnType(column: Int): ColumnType.ColumnType =
    ColumnType(nativeColumnType(ptr, column))

  def columnEncoding(column: Int): ColumnEncoding.ColumnEncoding =
    ColumnEncoding(nativeColumnEncoding(ptr, column))

  def columnCompression(column: Int): ColumnCompression.ColumnCompression =
    ColumnCompression(nativeColumnCompression(ptr, column))

  def isNull(column: Int): Boolean = nativeIsNull(ptr, column)

  def getBoolean(column: Int): Boolean = nativeGetBoolean(ptr, column)

  def getInt(column: Int): Int = nativeGetInt(ptr, column)

  def getLong(column: Int): Long = nativeGetLong(ptr, column)

  def getString(column: Int): String = nativeGetString(ptr, column)

  def getStringBytes(column: Int): Array[Byte] = nativeGetStringBytes(ptr, column)

  @native private def nativeNew(path: String): Long = ???

  @native private def nativeNewMatching(path: String, predicate: Predicate.Pointer): Pointer = ???

  @native private def nativeFree(reader: Pointer): Unit = ???

  @native private def nativeColumnCount(reader: Pointer): Int = ???

  @native private def nativeRowCount(reader: Pointer): Long = ???

  @native private def nativeRewind(reader: Pointer): Unit = ???

  @native private def nativeNext(reader: Pointer): Boolean = ???

  @native private def nativeColumnType(reader: Pointer, index: Int): Int = ???

  @native private def nativeColumnEncoding(reader: Pointer, index: Int): Int = ???

  @native private def nativeColumnCompression(reader: Pointer, index: Int): Int = ???

  @native private def nativeIsNull(reader: Pointer, index: Int): Boolean = ???

  @native private def nativeGetBoolean(reader: Pointer, index: Int): Boolean = ???

  @native private def nativeGetInt(reader: Pointer, index: Int): Int = ???

  @native private def nativeGetLong(reader: Pointer, index: Int): Long = ???

  @native private def nativeGetString(reader: Pointer, index: Int): String = ???

  @native private def nativeGetStringBytes(reader: Pointer, index: Int): Array[Byte] = ???
}