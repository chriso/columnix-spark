package zcs.jni

sealed trait Filter

case class LongEquals(column: Int, value: Long) extends Filter

sealed trait Operator extends Filter {

  def head: Filter
  def tail: Seq[Filter]
  def operands: Array[Filter] = (head +: tail).toArray
}

case class And(head: Filter, tail: Filter*) extends Operator

case class Or(head: Filter, tail: Filter*) extends Operator
