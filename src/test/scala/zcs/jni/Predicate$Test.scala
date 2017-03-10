package zcs.jni

class Predicate$Test extends Test {

  behavior of "Predicate$"

  it should "filter by boolean" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Boolean, "foo")
      writer.putBoolean(0, true)
      writer.putBoolean(0, false)
      writer.finish()
    }

    val expected = Seq(
      BooleanEquals(0, true) -> Seq(true),
      BooleanEquals(0, false) -> Seq(false),
      Or(BooleanEquals(0, false), BooleanEquals(0, true)) -> Seq(true, false),
      And(BooleanEquals(0, false), BooleanEquals(0, true)) -> Nil
    )

    for ((filter, result) <- expected) {
      withReader(file, Some(filter)) { reader =>
        reader.collect[Boolean](0).flatten shouldEqual result
      }
    }
  }

  it should "filter by int" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Int, "foo")
      for (i <- 1 to 10)
        writer.putInt(0, i)
      writer.finish()
    }

    val expected = Seq(
      IntEquals(0, 3) -> Seq(3),
      IntEquals(0, 999) -> Nil,
      Not(IntEquals(0, 3)) -> Seq(1, 2, 4, 5, 6, 7, 8, 9, 10),
      IntGreaterThan(0, 7) -> Seq(8, 9, 10),
      IntLessThan(0, 3) -> Seq(1, 2),
      Or(IntEquals(0, 3), IntEquals(0, 7), IntEquals(0, 9)) -> Seq(3, 7, 9),
      And(IntGreaterThan(0, 3), IntLessThan(0, 7)) -> Seq(4, 5, 6)
    )

    for ((filter, result) <- expected) {
      withReader(file, Some(filter)) { reader =>
        reader.collect[Int](0).flatten shouldEqual result
      }
    }
  }

  it should "filter by long" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Long, "foo")
      for (i <- 1L to 10L)
        writer.putLong(0, i)
      writer.finish()
    }

    val expected = Seq(
      LongEquals(0, 3L) -> Seq(3L),
      LongEquals(0, 999L) -> Nil,
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

  it should "filter by string" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.String, "foo")
      writer.putString(0, "a")
      writer.putString(0, "b")
      writer.putString(0, "c")
      writer.putString(0, "foo")
      writer.putString(0, "FOO")
      writer.finish()
    }

    val expected = Seq(
      StringEquals(0, "foo") -> Seq("foo"),
      StringEquals(0, "foo", caseSensitive = false) -> Seq("foo", "FOO"),
      StringEquals(0, "bar") -> Nil,
      StringGreaterThan(0, "b") -> Seq("c", "foo"),
      StringGreaterThan(0, "b", caseSensitive = false) -> Seq("c", "foo", "FOO"),
      StringLessThan(0, "b") -> Seq("a", "FOO"),
      StringLessThan(0, "b", caseSensitive = false) -> Seq("a"),
      StringContains(0, "foo", StringLocation.Start) -> Seq("foo"),
      StringContains(0, "foo", StringLocation.Start, caseSensitive = false) -> Seq("foo", "FOO"),
      StringContains(0, "f", StringLocation.Start) -> Seq("foo"),
      StringContains(0, "o", StringLocation.Start) -> Nil,
      StringContains(0, "foo", StringLocation.End) -> Seq("foo"),
      StringContains(0, "foo", StringLocation.End, caseSensitive = false) -> Seq("foo", "FOO"),
      StringContains(0, "o", StringLocation.End) -> Seq("foo"),
      StringContains(0, "f", StringLocation.End) -> Nil,
      StringContains(0, "foo", StringLocation.Any) -> Seq("foo"),
      StringContains(0, "foo", StringLocation.Any, caseSensitive = false) -> Seq("foo", "FOO"),
      StringContains(0, "f", StringLocation.Any) -> Seq("foo"),
      StringContains(0, "o", StringLocation.Any) -> Seq("foo")
    )

    for ((filter, result) <- expected) {
      withReader(file, Some(filter)) { reader =>
        reader.collect[String](0).flatten shouldEqual result
      }
    }
  }

  it should "filter nulls" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Long, "foo")
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