package com.columnix.spark

import com.columnix.file.{FileReader, Filter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{TaskContext, TaskKilledException}

case class RowIterator(context: TaskContext,
                       reader: FileReader,
                       columns: Array[Int],
                       dataTypes: Array[DataType]) extends Iterator[InternalRow] {

  private[this] val mutableRow =
    new SpecificInternalRow(columns map dataTypes)

  private[this] val setters =
    columns.zipWithIndex.map { case (in, out) => makeSetter(in, out) }

  private[this] val columnCount = columns.length

  def next: InternalRow = {
    var i = 0
    while (i < columnCount) {
      setters(i)()
      i += 1
    }
    mutableRow
  }

  def hasNext: Boolean = {
    if (context != null && context.isInterrupted) {
      reader.close()
      throw new TaskKilledException
    }

    if (reader.next) true
    else {
      reader.close()
      false
    }
  }

  private def makeSetter(in: Int, out: Int) = {
    dataTypes(in) match {
      case BooleanType =>
        () =>
          if (reader.isNull(in)) mutableRow.setNullAt(out)
          else mutableRow.setBoolean(out, reader.getBoolean(in))

      case IntegerType =>
        () =>
          if (reader.isNull(in)) mutableRow.setNullAt(out)
          else mutableRow.setInt(out, reader.getInt(in))

      case LongType =>
        () =>
          if (reader.isNull(in)) mutableRow.setNullAt(out)
          else mutableRow.setLong(out, reader.getLong(in))

      case StringType =>
        () =>
          if (reader.isNull(in)) mutableRow.setNullAt(out)
          else mutableRow.update(out, UTF8String.fromBytes(reader.getStringBytes(in)))
    }
  }
}

case class RepeatIterator(count: Long)
  extends Iterator[InternalRow] {

  private[this] var remaining = count

  val next: InternalRow = new SpecificInternalRow

  def hasNext: Boolean =
    if (remaining == 0) false
    else {
      remaining -= 1
      true
    }
}

object RowIterator {

  def build(context: TaskContext,
            path: String,
            filter: Option[Filter],
            columns: Array[Int],
            dataTypes: Array[DataType]): Iterator[InternalRow] = {

    val reader = new FileReader(path, filter)

    if (columns.isEmpty) {
      val rowCount = reader.rowCount
      reader.close()
      RepeatIterator(rowCount)
    } else {
      if (context != null) {
        context.addTaskCompletionListener(_ => reader.close())
        context.addTaskFailureListener((_, _) => reader.close())
      }
      RowIterator(context, reader, columns, dataTypes)
    }
  }
}
