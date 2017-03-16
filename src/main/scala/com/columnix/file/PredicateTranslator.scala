package com.columnix.file

import com.columnix.jni.Predicate

private[file] object PredicateTranslator {

  private[this] val native = new Predicate

  def fromFilter(filter: Filter): Predicate.Pointer = filter match {
    case IsNull(column) => native.isNull(column)
    case IsNotNull(column) => native.negate(native.isNull(column))
    // operators
    case Not(operand) => native.negate(fromFilter(operand))
    case and: And => native.and(and.operands map fromFilter)
    case or: Or => native.or(or.operands map fromFilter)
    // boolean
    case BooleanEquals(column, value) => native.booleanEquals(column, value)
    // int
    case IntEquals(column, value) => native.intEquals(column, value)
    case IntGreaterThan(column, value) => native.intGreaterThan(column, value)
    case IntLessThan(column, value) => native.intLessThan(column, value)
    // long
    case LongEquals(column, value) => native.longEquals(column, value)
    case LongGreaterThan(column, value) => native.longGreaterThan(column, value)
    case LongLessThan(column, value) => native.longLessThan(column, value)
    // float
    case FloatEquals(column, value) => native.floatEquals(column, value)
    case FloatGreaterThan(column, value) => native.floatGreaterThan(column, value)
    case FloatLessThan(column, value) => native.floatLessThan(column, value)
    // double
    case DoubleEquals(column, value) => native.doubleEquals(column, value)
    case DoubleGreaterThan(column, value) => native.doubleGreaterThan(column, value)
    case DoubleLessThan(column, value) => native.doubleLessThan(column, value)
    // string
    case StringEquals(column, value, caseSensitive) =>
      native.stringEquals(column, value, caseSensitive)
    case StringGreaterThan(column, value, caseSensitive) =>
      native.stringGreaterThan(column, value, caseSensitive)
    case StringLessThan(column, value, caseSensitive) =>
      native.stringLessThan(column, value, caseSensitive)
    case StringContains(column, value, location, caseSensitive) =>
      native.stringContains(column, value, location.id, caseSensitive)
  }
}
