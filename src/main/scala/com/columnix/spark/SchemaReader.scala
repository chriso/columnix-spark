package com.columnix.spark

import com.columnix.file.FileReader
import com.columnix.jni.ColumnType
import org.apache.spark.sql.types._

case object SchemaReader {

  def read(path: String): StructType = {
    val reader = new FileReader(path)
    try StructType(readFields(reader))
    finally reader.close()
  }

  private def readFields(reader: FileReader) =
    for {
      i <- 0 until reader.columnCount
      name = reader.columnName(i)
      fieldType = dataType(reader.columnType(i))
    } yield StructField(name, fieldType, nullable = true)

  private def dataType(columnType: ColumnType.ColumnType) =
    columnType match {
      case ColumnType.Boolean => BooleanType
      case ColumnType.Int => IntegerType
      case ColumnType.Long => LongType
      case ColumnType.String => StringType
    }
}
