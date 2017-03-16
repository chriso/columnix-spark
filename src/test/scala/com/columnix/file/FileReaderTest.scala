package com.columnix.file

import com.columnix.Test
import com.columnix.jni.ColumnType

class FileReaderTest extends Test {

  behavior of "FileReader"

  it should "have idempotent close()" in test { file =>
    empty(file)
    withReader(file) { reader =>
      reader.close()
      reader.close()
    }
  }

  it should "provide access to string bytes" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.String, "foo")
      writer.putString(0, "foo")
      writer.finish()
    }

    withReader(file) { reader =>
      reader.next shouldEqual true
      reader.getStringBytes(0) shouldEqual Array('f', 'o', 'o')
    }
  }

  it should "read metadata" in test { file =>
    withWriter(file) { writer =>
      writer.setMetadata("foo")
      writer.finish()
    }
    withReader(file) { reader =>
      reader.metadata shouldEqual Some("foo")
    }
  }

  it should "return none if a file has no metadata" in test { file =>
    empty(file)
    withReader(file) { reader =>
      reader.metadata shouldEqual None
    }
  }

  it should "fail with an NPE after close()" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Boolean, "bool")
      writer.addColumn(ColumnType.Int, "int")
      writer.addColumn(ColumnType.Long, "long")
      writer.addColumn(ColumnType.Float, "float")
      writer.addColumn(ColumnType.Double, "double")
      writer.addColumn(ColumnType.String, "str")
      writer.finish()
    }
    withReader(file) { reader =>
      reader.close()
      a[NullPointerException] should be thrownBy reader.rowCount
      a[NullPointerException] should be thrownBy reader.rewind()
      a[NullPointerException] should be thrownBy reader.next
      a[NullPointerException] should be thrownBy reader.metadata
      a[NullPointerException] should be thrownBy reader.columnName(0)
      a[NullPointerException] should be thrownBy reader.columnType(0)
      a[NullPointerException] should be thrownBy reader.columnEncoding(0)
      a[NullPointerException] should be thrownBy reader.columnCompression(0)
      a[NullPointerException] should be thrownBy reader.isNull(0)
      a[NullPointerException] should be thrownBy reader.getBoolean(0)
      a[NullPointerException] should be thrownBy reader.getInt(1)
      a[NullPointerException] should be thrownBy reader.getLong(2)
      a[NullPointerException] should be thrownBy reader.getFloat(3)
      a[NullPointerException] should be thrownBy reader.getDouble(4)
      a[NullPointerException] should be thrownBy reader.getString(5)
      a[NullPointerException] should be thrownBy reader.getStringBytes(5)
    }
  }

  it should "fail if a column index is out of bounds" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Boolean, "bool")
      writer.addColumn(ColumnType.Int, "int")
      writer.addColumn(ColumnType.Long, "long")
      writer.addColumn(ColumnType.Float, "float")
      writer.addColumn(ColumnType.Double, "double")
      writer.addColumn(ColumnType.String, "str")
      writer.finish()
    }
    withReader(file) { reader =>
      an[IndexOutOfBoundsException] should be thrownBy reader.columnName(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.columnType(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.columnEncoding(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.columnCompression(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.isNull(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.getBoolean(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.getInt(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.getLong(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.getFloat(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.getDouble(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.getString(-1)
      an[IndexOutOfBoundsException] should be thrownBy reader.getStringBytes(-1)

      an[IndexOutOfBoundsException] should be thrownBy reader.columnName(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.columnType(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.columnEncoding(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.columnCompression(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.isNull(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.getBoolean(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.getInt(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.getLong(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.getFloat(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.getDouble(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.getString(6)
      an[IndexOutOfBoundsException] should be thrownBy reader.getStringBytes(6)
    }
  }

}