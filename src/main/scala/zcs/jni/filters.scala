package zcs.jni

sealed trait Filter

case class Equals(column: Int, value: Long) extends Filter
