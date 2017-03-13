package com.columnix.jni

private[jni] object Predicate {

  System.loadLibrary("columnix")

  type Pointer = Long

  def fromFilter(filter: Filter): Pointer = filter match {
    case IsNull(column) => cNull(column)
    case IsNotNull(column) => cNegate(cNull(column))
    // operators
    case Not(operand) => cNegate(fromFilter(operand))
    case and: And => cAnd(and.operands map fromFilter)
    case or: Or => cOr(or.operands map fromFilter)
    // boolean
    case BooleanEquals(column, value) => cBooleanEquals(column, value)
    // int
    case IntEquals(column, value) => cIntEquals(column, value)
    case IntGreaterThan(column, value) => cIntGreaterThan(column, value)
    case IntLessThan(column, value) => cIntLessThan(column, value)
    // long
    case LongEquals(column, value) => cLongEquals(column, value)
    case LongGreaterThan(column, value) => cLongGreaterThan(column, value)
    case LongLessThan(column, value) => cLongLessThan(column, value)
    // string
    case StringEquals(column, value, caseSensitive) =>
      cStringEquals(column, value, caseSensitive)
    case StringGreaterThan(column, value, caseSensitive) =>
      cStringGreaterThan(column, value, caseSensitive)
    case StringLessThan(column, value, caseSensitive) =>
      cStringLessThan(column, value, caseSensitive)
    case StringContains(column, value, location, caseSensitive) =>
      cStringContains(column, value, location.id, caseSensitive)
  }

  @native private def cBooleanEquals(column: Int, value: Boolean): Pointer = ???

  @native private def cIntEquals(column: Int, value: Int): Pointer = ???

  @native private def cIntGreaterThan(column: Int, value: Int): Pointer = ???

  @native private def cIntLessThan(column: Int, value: Int): Pointer = ???

  @native private def cLongEquals(column: Int, value: Long): Pointer = ???

  @native private def cLongGreaterThan(column: Int, value: Long): Pointer = ???

  @native private def cLongLessThan(column: Int, value: Long): Pointer = ???

  @native private def cStringEquals(column: Int, value: String,
                                    caseSensitive: Boolean): Pointer = ???

  @native private def cStringGreaterThan(column: Int, value: String,
                                         caseSensitive: Boolean): Pointer = ???

  @native private def cStringLessThan(column: Int, value: String,
                                      caseSensitive: Boolean): Pointer = ???

  @native private def cStringContains(column: Int, value: String, location: Int,
                                      caseSensitive: Boolean): Pointer = ???

  @native private def cAnd(predicates: Array[Pointer]): Pointer = ???

  @native private def cOr(predicates: Array[Pointer]): Pointer = ???

  @native private def cNegate(predicate: Pointer): Pointer = ???

  @native private def cNull(column: Int): Pointer = ???

  @native private def cFree(predicate: Pointer): Unit = ???
}
