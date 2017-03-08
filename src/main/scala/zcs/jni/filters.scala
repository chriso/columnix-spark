package zcs.jni

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

case class LongEquals(column: Int, value: Long) extends Filter

case class LongGreaterThan(column: Int, value: Long) extends Filter

case class LongLessThan(column: Int, value: Long) extends Filter

case class StringEquals(column: Int, value: String,
                        caseSensitive: Boolean = true) extends Filter

case class StringGreaterThan(column: Int, value: String,
                             caseSensitive: Boolean = true) extends Filter

case class StringLessThan(column: Int, value: String,
                          caseSensitive: Boolean = true) extends Filter

case class StringContains(column: Int, value: String,
                          location: StringLocation.StringLocation,
                          caseSensitive: Boolean = true) extends Filter
