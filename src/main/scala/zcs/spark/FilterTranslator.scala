package zcs.spark

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{sources => spark}
import zcs.jni._

case class FilterTranslator(columns: Map[String, Int], types: IndexedSeq[DataType]) {

  def translateFilters(filters: Seq[spark.Filter]): Option[Filter] =
    filters match {
      case head +: tail => Some(And(translate(head), tail map translate: _*))
      case Nil => None
    }

  private def translate(filter: spark.Filter): Filter = filter match {
    case spark.IsNull(name) => IsNull(columns(name))
    case spark.IsNotNull(name) => IsNotNull(columns(name))
  }
}
