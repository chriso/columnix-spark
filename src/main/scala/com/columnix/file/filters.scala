package com.columnix.file

import com.columnix.jni.StringLocation

sealed trait Filter

case class IsNull(column: Int) extends Filter

case class IsNotNull(column: Int) extends Filter

case class Not(filter: Filter) extends Filter

sealed trait Operator extends Filter {

  def head: Filter

  def tail: Seq[Filter]

  def operands: Array[Filter] = (head +: tail).toArray
}

case class And(head: Filter, tail: Filter*) extends Operator

case class Or(head: Filter, tail: Filter*) extends Operator

case class BooleanEquals(column: Int, value: Boolean) extends Filter

case class IntEquals(column: Int, value: Int) extends Filter

case class IntGreaterThan(column: Int, value: Int) extends Filter

case class IntLessThan(column: Int, value: Int) extends Filter

case class LongEquals(column: Int, value: Long) extends Filter

case class LongGreaterThan(column: Int, value: Long) extends Filter

case class LongLessThan(column: Int, value: Long) extends Filter

case class FloatEquals(column: Int, value: Float) extends Filter

case class FloatGreaterThan(column: Int, value: Float) extends Filter

case class FloatLessThan(column: Int, value: Float) extends Filter

case class DoubleEquals(column: Int, value: Double) extends Filter

case class DoubleGreaterThan(column: Int, value: Double) extends Filter

case class DoubleLessThan(column: Int, value: Double) extends Filter

case class StringEquals(column: Int, value: String,
                        caseSensitive: Boolean = true) extends Filter

case class StringGreaterThan(column: Int, value: String,
                             caseSensitive: Boolean = true) extends Filter

case class StringLessThan(column: Int, value: String,
                          caseSensitive: Boolean = true) extends Filter

case class StringContains(column: Int, value: String,
                          location: StringLocation.StringLocation,
                          caseSensitive: Boolean = true) extends Filter
