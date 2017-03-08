package zcs.jni

class PredicateTest extends Test {

  behavior of "Predicate"

  it should "filter by long value" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Long)
      for (i <- 1L to 10L)
        writer.putLong(0, i)
      writer.finish()
    }

    val expected = Seq(
      LongEquals(0, 3L) -> Seq(3L),
      Not(LongEquals(0, 3L)) -> Seq(1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
      LongGreaterThan(0, 7L) -> Seq(8L, 9L, 10L),
      LongLessThan(0, 3L) -> Seq(1L, 2L),
      Or(LongEquals(0, 3L), LongEquals(0, 7L), LongEquals(0, 9L)) -> Seq(3L, 7L, 9L),
      And(LongGreaterThan(0, 3L), LongLessThan(0, 7L)) -> Seq(4L, 5L, 6L)
    )

    for ((filter, result) <- expected) {
      withReader(file, Some(filter)) { reader =>
        reader.collect[Long](0).flatten shouldEqual result
      }
    }
  }

  it should "filter nulls" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Long)
      writer.putLong(0, 1L)
      writer.putNull(0)
      writer.putLong(0, 2L)
      writer.putNull(0)
      writer.putLong(0, 3L)
      writer.putNull(0)
      writer.finish()
    }

    val expected = Seq(
      IsNull(0) -> Seq(None, None, None),
      IsNotNull(0) -> Seq(Some(1L), Some(2L), Some(3L)),
      And(IsNotNull(0), LongEquals(0, 3L)) -> Seq(Some(3L))
    )

    for ((filter, result) <- expected) {
      withReader(file, Some(filter)) { reader =>
        reader.collect[Long](0) shouldEqual result
      }
    }
  }

}