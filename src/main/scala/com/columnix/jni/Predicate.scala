package com.columnix.jni

private[columnix] class Predicate {

  System.loadLibrary("columnix")

  import Predicate.Pointer

  @native def booleanEquals(column: Int, value: Boolean): Pointer = ???

  @native def intEquals(column: Int, value: Int): Pointer = ???

  @native def intGreaterThan(column: Int, value: Int): Pointer = ???

  @native def intLessThan(column: Int, value: Int): Pointer = ???

  @native def longEquals(column: Int, value: Long): Pointer = ???

  @native def longGreaterThan(column: Int, value: Long): Pointer = ???

  @native def longLessThan(column: Int, value: Long): Pointer = ???

  @native def stringEquals(column: Int, value: String,
                           caseSensitive: Boolean): Pointer = ???

  @native def stringGreaterThan(column: Int, value: String,
                                caseSensitive: Boolean): Pointer = ???

  @native def stringLessThan(column: Int, value: String,
                             caseSensitive: Boolean): Pointer = ???

  @native def stringContains(column: Int, value: String, location: Int,
                             caseSensitive: Boolean): Pointer = ???

  @native def and(predicates: Array[Pointer]): Pointer = ???

  @native def or(predicates: Array[Pointer]): Pointer = ???

  @native def negate(predicate: Pointer): Pointer = ???

  @native def isNull(column: Int): Pointer = ???
}

object Predicate {

  type Pointer = Long
}
