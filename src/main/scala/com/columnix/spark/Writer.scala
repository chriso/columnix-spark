package com.columnix.spark

import com.columnix.jni.{ColumnCompression, ColumnType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

case class Writer(path: String, data: DataFrame) {

  private[this] val writer = new com.columnix.jni.Writer(path)

  private[this] val fields = data.schema.fields

  private[this] val columnCount = fields.length

  private[this] val getSet = fields.map(_.dataType).zipWithIndex map {
    case (_: BooleanType, i) =>
      (row: Row) => writer.putBoolean(i, row.getBoolean(i))

    case (_: IntegerType, i) =>
      (row: Row) => writer.putInt(i, row.getInt(i))

    case (_: LongType, i) =>
      (row: Row) => writer.putLong(i, row.getLong(i))

    case (_: StringType, i) =>
      (row: Row) => writer.putString(i, row.getString(i))
  }

  for (field <- fields) {
    val columnType = field.dataType match {
      case BooleanType => ColumnType.Boolean
      case IntegerType => ColumnType.Int
      case LongType => ColumnType.Long
      case StringType => ColumnType.String
      case _ =>
        throw new RuntimeException(s"unsupported column type: $field")
    }
    writer.addColumn(columnType, field.name, compression = ColumnCompression.LZ4HC)
  }

  def write(): Unit = {
    data.foreach { row =>
      var i = 0
      while (i < columnCount) {
        if (row.isNullAt(i)) writer.putNull(i)
        else getSet(i)(row)
        i += 1
      }
    }

    writer.finish(sync = true)
  }

  def close(): Unit = writer.close()
}
