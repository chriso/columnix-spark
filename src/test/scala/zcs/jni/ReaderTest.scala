package zcs.jni

class ReaderTest extends Test {

  behavior of "Reader"

  it should "have idempotent close()" in test { file =>
    withWriter(file)(_.finish())
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
}