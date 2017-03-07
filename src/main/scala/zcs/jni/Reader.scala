package zcs.jni

class Reader(path: String, filter: Option[Filter] = None) {

  System.loadLibrary("zcs")

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

  @native private def nativeNew(path: String): Long = ???

  @native private def nativeNewMatching(path: String, predicate: Long): Long = ???

  @native private def nativeFree(ptr: Long): Unit = ???

  @native private def nativeColumnCount(ptr: Long): Int = ???

  @native private def nativeRowCount(ptr: Long): Long = ???

  @native private def nativeRewind(ptr: Long): Unit = ???

  @native private def nativeNext(ptr: Long): Boolean = ???

  @native private def nativeColumnType(ptr: Long, index: Int): Int = ???

  @native private def nativeColumnEncoding(ptr: Long, index: Int): Int = ???

  @native private def nativeColumnCompression(ptr: Long, index: Int): Int = ???

  @native private def nativeIsNull(ptr: Long, index: Int): Boolean = ???

  @native private def nativeGetBoolean(ptr: Long, index: Int): Boolean = ???

  @native private def nativeGetInt(ptr: Long, index: Int): Int = ???

  @native private def nativeGetLong(ptr: Long, index: Int): Long = ???

  @native private def nativeGetString(ptr: Long, index: Int): String = ???
}