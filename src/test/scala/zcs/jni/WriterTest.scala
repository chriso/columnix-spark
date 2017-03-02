package zcs.jni

class WriterTest extends Test {

  behavior of "Writer"

  it should "write column values to a file" in {
    withTemporaryFile { path =>
      withWriter(path) { writer =>
        writer.addColumn(ColumnType.Boolean)
        writer.addColumn(ColumnType.Int)
        writer.addColumn(ColumnType.Long)
        writer.addColumn(ColumnType.String)
        writer.put(0, Seq(Some(true), None, Some(false), None, None))
        writer.put(1, Seq(Some(Int.MinValue), Some(Int.MaxValue), None, Some(-1), Some(11)))
        writer.put(2, Seq(Some(Long.MinValue), Some(Long.MaxValue), None, Some(-1L), Some(22L)))
        writer.put(3, Seq(None, None, Some("foo"), Some("bar"), Some(null)))
        writer.finish()
      }
      withReader(path) { reader =>
        reader.columnCount shouldEqual 4
        reader.rowCount shouldEqual 5
        reader.columnType(0) shouldEqual ColumnType.Boolean
        reader.columnType(1) shouldEqual ColumnType.Int
        reader.columnType(2) shouldEqual ColumnType.Long
        reader.columnType(3) shouldEqual ColumnType.String
        reader.collect[Boolean](0) shouldEqual Seq(Some(true), None, Some(false), None, None)
        reader.collect[Int](1) shouldEqual Seq(Some(Int.MinValue), Some(Int.MaxValue), None, Some(-1), Some(11))
        reader.collect[Long](2) shouldEqual Seq(Some(Long.MinValue), Some(Long.MaxValue), None, Some(-1L), Some(22L))
        reader.collect[String](3) shouldEqual Seq(None, None, Some("foo"), Some("bar"), None)
      }
    }
  }
}
