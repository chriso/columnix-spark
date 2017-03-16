package com.columnix.spark

import java.nio.file.Path

import com.columnix.Test
import com.columnix.jni.ColumnType
import org.apache.spark.sql.types._

class SchemaReader$Test extends Test {

  behavior of "SchemaReader$"

  private def schema(path: Path) =
    SchemaReader.read(path.toString)

  it should "read empty files" in test { file =>
    empty(file)
    schema(file) shouldEqual StructType(Seq())
  }

  it should "read the schema from a file" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Boolean, "bool1")
      writer.addColumn(ColumnType.Int, "int2")
      writer.addColumn(ColumnType.Long, "long3")
      writer.addColumn(ColumnType.Float, "float4")
      writer.addColumn(ColumnType.Double, "double5")
      writer.addColumn(ColumnType.String, "str6")
      writer.finish()
    }
    schema(file) shouldEqual StructType(Seq(
      StructField("bool1", BooleanType, nullable = true),
      StructField("int2", IntegerType, nullable = true),
      StructField("long3", LongType, nullable = true),
      StructField("float4", FloatType, nullable = true),
      StructField("double5", DoubleType, nullable = true),
      StructField("str6", StringType, nullable = true)
    ))
  }
}
