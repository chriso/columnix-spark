package com.columnix.spark

import com.columnix.file.FileWriter
import com.columnix.jni.{ColumnCompression, ColumnType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types._

case class RowWriter(path: String, schema: StructType,
                     parameters: Map[String, String]) extends OutputWriter {

  private[this] val writer = new FileWriter(path)

  private[this] val dataTypes = schema.fields map (_.dataType)

  private[this] val columnCount = schema.length

  private[this] val getSet = dataTypes map {
    case _: BooleanType =>
      (row: Row, i: Int) => writer.putBoolean(i, row.getBoolean(i))

    case _: IntegerType =>
      (row: Row, i: Int) => writer.putInt(i, row.getInt(i))

    case _: LongType =>
      (row: Row, i: Int) => writer.putLong(i, row.getLong(i))

    case _: FloatType =>
      (row: Row, i: Int) => writer.putFloat(i, row.getFloat(i))

    case _: DoubleType =>
      (row: Row, i: Int) => writer.putDouble(i, row.getDouble(i))

    case _: StringType =>
      (row: Row, i: Int) => writer.putString(i, row.getString(i))
  }

  for (field <- schema.fields) {
    val columnType = field.dataType match {
      case BooleanType => ColumnType.Boolean
      case IntegerType => ColumnType.Int
      case LongType => ColumnType.Long
      case FloatType => ColumnType.Float
      case DoubleType => ColumnType.Double
      case StringType => ColumnType.String
      case _ =>
        throw new RuntimeException(s"unsupported column type: $field")
    }
    writer.addColumn(columnType, field.name, compression = ColumnCompression.LZ4)
  }

  def write(row: Row): Unit = {
    var i = 0
    while (i < columnCount) {
      if (row.isNullAt(i)) writer.putNull(i)
      else getSet(i)(row, i)
      i += 1
    }
  }

  def close(): Unit = {
    writer.finish(sync = true)
    writer.close()
  }
}
