package com.columnix.spark

import com.columnix.jni.Reader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow

case class CountIterator(reader: Reader) extends Iterator[InternalRow] {

  private[this] var remaining = reader.rowCount

  val next: InternalRow = new SpecificInternalRow()

  def hasNext: Boolean =
    if (remaining == 0) false
    else {
      remaining -= 1
      true
    }
}
