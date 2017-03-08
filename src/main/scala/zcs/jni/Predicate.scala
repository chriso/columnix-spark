package zcs.jni

object Predicate {

  System.loadLibrary("zcs")

  type Pointer = Long

  private[zcs] def fromFilter(filter: Filter): Pointer = filter match {
    case IsNull(column) => nativeNull(column)
    case IsNotNull(column) => nativeNegate(nativeNull(column))
    case Not(operand) => nativeNegate(fromFilter(operand))
    case and: And => nativeAnd(and.operands map fromFilter)
    case or: Or => nativeOr(or.operands map fromFilter)
    case LongEquals(column, value) => nativeLongEquals(column, value)
    case LongGreaterThan(column, value) => nativeLongGreaterThan(column, value)
    case LongLessThan(column, value) => nativeLongLessThan(column, value)
  }

  @native private def nativeLongEquals(column: Int, value: Long): Pointer = ???

  @native private def nativeLongGreaterThan(column: Int, value: Long): Pointer = ???

  @native private def nativeLongLessThan(column: Int, value: Long): Pointer = ???

  @native private def nativeAnd(predicates: Array[Pointer]): Pointer = ???

  @native private def nativeOr(predicates: Array[Pointer]): Pointer = ???

  @native private def nativeNegate(predicate: Pointer): Pointer = ???

  @native private def nativeNull(column: Int): Pointer = ???

  @native private def nativeFree(predicate: Pointer): Unit = ???
}
