package com.columnix.spark

import com.columnix.file._
import com.columnix.jni.StringLocation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{sources => spark}

case class FilterTranslator(columns: Map[String, Int], types: Array[DataType]) {

  def translateFilters(filters: spark.Filter*): Option[Filter] =
    filters match {
      case Nil => None
      case Seq(head) => Some(translate(head))
      case head +: tail => Some(And(translate(head), tail map translate: _*))
    }

  private def translate(filter: spark.Filter): Filter = filter match {
    case spark.IsNull(name) => IsNull(columns(name))
    case spark.IsNotNull(name) => IsNotNull(columns(name))
    case spark.Not(operand) => Not(translate(operand))
    case spark.And(left, right) => And(translate(left), translate(right))
    case spark.Or(left, right) => Or(translate(left), translate(right))
    case spark.EqualTo(name, value) => translateEquals(columns(name), value)
    case spark.GreaterThan(name, value) => translateGreaterThan(columns(name), value)
    case spark.GreaterThanOrEqual(name, value) => Not(translateLessThan(columns(name), value))
    case spark.LessThan(name, value) => translateLessThan(columns(name), value)
    case spark.LessThanOrEqual(name, value) => Not(translateGreaterThan(columns(name), value))
    case spark.EqualNullSafe(name, value) =>
      val column = columns(name)
      And(IsNotNull(column), translateEquals(column, value))
    case spark.In(name, values) =>
      val column = columns(name)
      val operands = values map (translateEquals(column, _))
      Or(operands.head, operands.tail: _*)
    case spark.StringStartsWith(name, value) =>
      StringContains(columns(name), value, StringLocation.Start)
    case spark.StringEndsWith(name, value) =>
      StringContains(columns(name), value, StringLocation.End)
    case spark.StringContains(name, value) =>
      StringContains(columns(name), value, StringLocation.Any)
  }

  private def translateEquals(column: Int, value: Any): Filter =
    types(column) match {
      case BooleanType => BooleanEquals(column, toBoolean(value))
      case IntegerType => IntEquals(column, toInt(value))
      case LongType => LongEquals(column, toLong(value))
      case FloatType => FloatEquals(column, toFloat(value))
      case DoubleType => DoubleEquals(column, toDouble(value))
      case StringType => StringEquals(column, toString(value))
    }

  private def translateGreaterThan(column: Int, value: Any): Filter =
    types(column) match {
      case IntegerType => IntGreaterThan(column, toInt(value))
      case LongType => LongGreaterThan(column, toLong(value))
      case FloatType => FloatGreaterThan(column, toFloat(value))
      case DoubleType => DoubleGreaterThan(column, toDouble(value))
      case StringType => StringGreaterThan(column, toString(value))
    }

  private def translateLessThan(column: Int, value: Any): Filter =
    types(column) match {
      case IntegerType => IntLessThan(column, toInt(value))
      case LongType => LongLessThan(column, toLong(value))
      case FloatType => FloatLessThan(column, toFloat(value))
      case DoubleType => DoubleLessThan(column, toDouble(value))
      case StringType => StringLessThan(column, toString(value))
    }

  private def toBoolean(value: Any): Boolean = value match {
    case bool: Boolean => bool
  }

  private def toInt(value: Any): Int = value match {
    case int: Int => int
  }

  private def toLong(value: Any): Long = value match {
    case long: Long => long
    case int: Int => int.toLong
  }

  private def toFloat(value: Any): Float = value match {
    case float: Float => float
  }

  private def toDouble(value: Any): Double = value match {
    case double: Double => double
  }

  private def toString(value: Any): String = value.toString
}
