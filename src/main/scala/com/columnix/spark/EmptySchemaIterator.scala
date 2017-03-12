package com.columnix.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.TaskContext
import com.columnix.jni.Reader

case class EmptySchemaIterator(reader: Reader) extends Iterator[InternalRow] {

  private[this] var remaining = reader.rowCount

  private[this] val row = new SpecificInternalRow()

  def next: InternalRow = row

  def hasNext: Boolean =
    if (remaining == 0) false
    else {
      remaining -= 1
      true
    }
}
