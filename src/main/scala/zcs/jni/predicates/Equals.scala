package zcs.jni.predicates

case class Equals(column: Int, value: Long) extends Predicate {

  ptr = nativeLongEquals(column, value)

  @native private def nativeLongEquals(column: Int, value: Long): Long = ???
}
