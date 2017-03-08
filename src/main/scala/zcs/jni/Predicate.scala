package zcs.jni

object Predicate {

  System.loadLibrary("zcs")

  type Pointer = Long

  private[zcs] def fromFilter(filter: Filter): Pointer = filter match {
    case LongEquals(column, value) => nativeLongEquals(column, value)
    case and: And => nativeAnd(and.operands map fromFilter)
    case or: Or => nativeOr(or.operands map fromFilter)
  }

  @native private def nativeLongEquals(column: Int, value: Long): Pointer = ???

  @native private def nativeAnd(predicates: Array[Pointer]): Pointer = ???

  @native private def nativeOr(predicates: Array[Pointer]): Pointer = ???

  @native private def nativeNegate(predicate: Pointer): Long = ???

  @native private def nativeFree(predicate: Pointer): Unit = ???
}
