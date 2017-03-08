package zcs.jni

object Predicate {

  System.loadLibrary("zcs")

  type Pointer = Long

  private[zcs] def fromFilter(filter: Filter): Pointer = filter match {
    case IsNull(column) => nativeNull(column)
    case IsNotNull(column) => nativeNegate(nativeNull(column))
    // operators
    case Not(operand) => nativeNegate(fromFilter(operand))
    case and: And => nativeAnd(and.operands map fromFilter)
    case or: Or => nativeOr(or.operands map fromFilter)
    // boolean
    case BooleanEquals(column, value) => nativeBooleanEquals(column, value)
    // long
    case LongEquals(column, value) => nativeLongEquals(column, value)
    case LongGreaterThan(column, value) => nativeLongGreaterThan(column, value)
    case LongLessThan(column, value) => nativeLongLessThan(column, value)
    // string
    case StringEquals(column, value, caseSensitive) =>
      nativeStringEquals(column, value, caseSensitive)
    case StringGreaterThan(column, value, caseSensitive) =>
      nativeStringGreaterThan(column, value, caseSensitive)
    case StringLessThan(column, value, caseSensitive) =>
      nativeStringLessThan(column, value, caseSensitive)
    case StringContains(column, value, location, caseSensitive) =>
      nativeStringContains(column, value, location.id, caseSensitive)
  }

  @native private def nativeBooleanEquals(column: Int, value: Boolean): Pointer = ???

  @native private def nativeLongEquals(column: Int, value: Long): Pointer = ???

  @native private def nativeLongGreaterThan(column: Int, value: Long): Pointer = ???

  @native private def nativeLongLessThan(column: Int, value: Long): Pointer = ???

  @native private def nativeStringEquals(column: Int, value: String,
                                         caseSensitive: Boolean): Pointer = ???

  @native private def nativeStringGreaterThan(column: Int, value: String,
                                              caseSensitive: Boolean): Pointer = ???

  @native private def nativeStringLessThan(column: Int, value: String,
                                           caseSensitive: Boolean): Pointer = ???

  @native private def nativeStringContains(column: Int, value: String, location: Int,
                                           caseSensitive: Boolean): Pointer = ???

  @native private def nativeAnd(predicates: Array[Pointer]): Pointer = ???

  @native private def nativeOr(predicates: Array[Pointer]): Pointer = ???

  @native private def nativeNegate(predicate: Pointer): Pointer = ???

  @native private def nativeNull(column: Int): Pointer = ???

  @native private def nativeFree(predicate: Pointer): Unit = ???
}
