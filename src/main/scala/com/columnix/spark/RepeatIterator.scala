package com.columnix.spark

import org.apache.spark.sql.catalyst.InternalRow

case class RepeatIterator(count: Long, next: InternalRow) extends Iterator[InternalRow] {

  private[this] var remaining = count

  def hasNext: Boolean =
    if (remaining == 0) false
    else {
      remaining -= 1
      true
    }
}
