package zcs.jni

sealed trait Filter

case class LongEquals(column: Int, value: Long) extends Filter
