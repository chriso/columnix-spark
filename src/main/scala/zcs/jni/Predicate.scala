package zcs.jni

object Predicate {

  System.loadLibrary("zcs")

  type Pointer = Long

  private[zcs] def fromFilter(filter: Filter): Pointer = filter match {
    case LongEquals(column, value) => nativeLongEquals(column, value)
  }

  @native private def nativeLongEquals(column: Int, value: Long): Pointer = ???

  @native private def nativeNegate(ptr: Pointer): Long = ???

  @native private def nativeNegate(predicate: Pointer): Long = ???

  @native private def nativeFree(predicate: Pointer): Unit = ???
}
