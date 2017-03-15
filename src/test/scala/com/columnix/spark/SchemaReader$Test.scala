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
      writer.addColumn(ColumnType.Int, "int1")
      writer.addColumn(ColumnType.Long, "long2")
      writer.addColumn(ColumnType.Boolean, "bool3")
      writer.addColumn(ColumnType.String, "str4")
      writer.finish()
    }
    schema(file) shouldEqual StructType(Seq(
      StructField("int1", IntegerType, nullable = true),
      StructField("long2", LongType, nullable = true),
      StructField("bool3", BooleanType, nullable = true),
      StructField("str4", StringType, nullable = true)
    ))
  }
}
