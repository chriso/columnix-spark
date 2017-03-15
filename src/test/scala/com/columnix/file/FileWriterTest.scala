package com.columnix.file

import com.columnix.Test
import com.columnix.file.implicits._
import com.columnix.jni.ColumnType

class FileWriterTest extends Test {

  behavior of "FileWriter"

  it should "have idempotent finish()" in test { file =>
    withWriter(file) { writer =>
      writer.finish()
      writer.finish()
    }
  }

  it should "have idempotent close()" in test { file =>
    withWriter(file) { writer =>
      writer.close()
      writer.close()
    }
  }

  it should "write files with no columns" in test { file =>
    withWriter(file)(_.finish())
    withReader(file) { reader =>
      reader.columnCount shouldEqual 0
      reader.rowCount shouldEqual 0
    }
  }

  it should "write files with no rows" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Int, "foo")
      writer.addColumn(ColumnType.Long, "bar")
      writer.finish()
    }
    withReader(file) { reader =>
      reader.columnCount shouldEqual 2
      reader.columnType(0) shouldEqual ColumnType.Int
      reader.columnType(1) shouldEqual ColumnType.Long
      reader.rowCount shouldEqual 0
    }
  }

  it should "write column values to a file" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Boolean, "bool")
      writer.addColumn(ColumnType.Int, "int")
      writer.addColumn(ColumnType.Long, "long")
      writer.addColumn(ColumnType.String, "string")
      writer.put(0, Seq(Some(true), None, Some(false), None, None))
      writer.put(1, Seq(Some(Int.MinValue), Some(Int.MaxValue), None, Some(-1), Some(11)))
      writer.put(2, Seq(Some(Long.MinValue), Some(Long.MaxValue), None, Some(-1L), Some(22L)))
      writer.put(3, Seq(None, None, Some("foo"), Some("bar"), Some(null)))
      writer.finish()
    }
    withReader(file) { reader =>
      reader.columnCount shouldEqual 4
      reader.rowCount shouldEqual 5
      reader.columnType(0) shouldEqual ColumnType.Boolean
      reader.columnType(1) shouldEqual ColumnType.Int
      reader.columnType(2) shouldEqual ColumnType.Long
      reader.columnType(3) shouldEqual ColumnType.String
      reader.columnName(0) shouldEqual "bool"
      reader.columnName(1) shouldEqual "int"
      reader.columnName(2) shouldEqual "long"
      reader.columnName(3) shouldEqual "string"
      reader.collect[Boolean](0) shouldEqual Seq(Some(true), None, Some(false), None, None)
      reader.collect[Int](1) shouldEqual Seq(Some(Int.MinValue), Some(Int.MaxValue), None, Some(-1), Some(11))
      reader.collect[Long](2) shouldEqual Seq(Some(Long.MinValue), Some(Long.MaxValue), None, Some(-1L), Some(22L))
      reader.collect[String](3) shouldEqual Seq(None, None, Some("foo"), Some("bar"), None)
    }
  }

  it should "fail with an NPE after close()" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Int, "int")
      writer.addColumn(ColumnType.Long, "long")
      writer.addColumn(ColumnType.Boolean, "bool")
      writer.addColumn(ColumnType.String, "str")
      writer.close()

      a[NullPointerException] should be thrownBy writer.finish()
      a[NullPointerException] should be thrownBy writer.setMetadata("foo")
      a[NullPointerException] should be thrownBy writer.addColumn(ColumnType.Int, "foo")
      a[NullPointerException] should be thrownBy writer.putNull(0)
      a[NullPointerException] should be thrownBy writer.putInt(0, 10)
      a[NullPointerException] should be thrownBy writer.putLong(1, 10L)
      a[NullPointerException] should be thrownBy writer.putBoolean(2, true)
      a[NullPointerException] should be thrownBy writer.putString(3, "foo")
    }
  }

  it should "fail if a column index is out of bounds" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Int, "int")
      writer.addColumn(ColumnType.Long, "long")
      writer.addColumn(ColumnType.Boolean, "bool")
      writer.addColumn(ColumnType.String, "str")

      an[IndexOutOfBoundsException] should be thrownBy writer.putNull(-1)
      an[IndexOutOfBoundsException] should be thrownBy writer.putInt(-1, 10)
      an[IndexOutOfBoundsException] should be thrownBy writer.putLong(-1, 10L)
      an[IndexOutOfBoundsException] should be thrownBy writer.putBoolean(-1, true)
      an[IndexOutOfBoundsException] should be thrownBy writer.putString(-1, "foo")

      an[IndexOutOfBoundsException] should be thrownBy writer.putNull(4)
      an[IndexOutOfBoundsException] should be thrownBy writer.putInt(4, 10)
      an[IndexOutOfBoundsException] should be thrownBy writer.putLong(4, 10L)
      an[IndexOutOfBoundsException] should be thrownBy writer.putBoolean(4, true)
      an[IndexOutOfBoundsException] should be thrownBy writer.putString(4, "foo")
    }
  }

}
