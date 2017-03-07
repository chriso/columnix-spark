package zcs.jni

object Predicate {

  System.loadLibrary("zcs")

  private[zcs] def fromFilter(filter: Filter): Long = filter match {
    case Equals(column, value) => nativeLongEquals(column, value)
  }

  @native private def nativeLongEquals(column: Int, value: Long): Long = ???

  @native private def nativeNegate(ptr: Long): Long = ???

  @native private def nativeFree(ptr: Long): Unit = ???
}
