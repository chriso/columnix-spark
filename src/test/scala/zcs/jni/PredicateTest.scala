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
      Or(LongEquals(0, 3L), LongEquals(0, 7L), LongEquals(0, 9L)) -> Seq(3L, 7L, 9L)
    )

    for ((filter, result) <- expected) {
      withReader(file, Some(filter)) { reader =>
        reader.collect[Long](0).flatten shouldEqual result
      }
    }
  }

}