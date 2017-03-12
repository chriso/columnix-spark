package com.columnix.spark

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{TaskContext, TaskKilledException}
import com.columnix.jni.Reader

case class RowIterator(context: TaskContext,
                       reader: Reader,
                       columns: Array[Int],
                       schema: StructType) extends Iterator[InternalRow] {

  private[this] val fieldTypes = schema.fields.map(_.dataType)

  private[this] val mutableRow = new SpecificInternalRow(fieldTypes)

  private[this] val setters = columns.zipWithIndex.map { case (in, out) => makeSetter(in, out) }

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
    if (context.isInterrupted) {
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
    fieldTypes(out) match {
      case BooleanType =>
        () => if (reader.isNull(in)) mutableRow.setNullAt(out)
        else mutableRow.setBoolean(out, reader.getBoolean(in))

      case IntegerType =>
        () => if (reader.isNull(in)) mutableRow.setNullAt(out)
        else mutableRow.setInt(out, reader.getInt(in))

      case LongType =>
        () => if (reader.isNull(in)) mutableRow.setNullAt(out)
        else mutableRow.setLong(out, reader.getLong(in))

      case StringType =>
        () => if (reader.isNull(in)) mutableRow.setNullAt(out)
        else mutableRow.update(out, UTF8String.fromBytes(reader.getStringBytes(in)))
    }
  }
}
