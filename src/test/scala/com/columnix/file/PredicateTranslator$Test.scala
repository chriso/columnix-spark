package com.columnix.file

import java.nio.file.Path

import com.columnix.Test
import com.columnix.file.implicits._
import com.columnix.jni._

class PredicateTranslator$Test extends Test {

  behavior of "PredicateTranslator$"

  private def checkFilters[T: Getter](file: Path, tests: (Filter, Seq[T])*) =
    for ((filter, expectedResult) <- tests) {
      withReader(file, Some(filter)) { reader =>
        reader.collect[T](0).flatten shouldEqual expectedResult
      }
    }

  it should "filter by boolean" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Boolean, "foo")
      writer.putBoolean(0, true)
      writer.putBoolean(0, false)
      writer.finish()
    }

    checkFilters(file,
      BooleanEquals(0, true) -> Seq(true),
      BooleanEquals(0, false) -> Seq(false),
      Or(BooleanEquals(0, false), BooleanEquals(0, true)) -> Seq(true, false),
      And(BooleanEquals(0, false), BooleanEquals(0, true)) -> Nil)
  }

  it should "filter by int" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Int, "foo")
      for (i <- 1 to 10)
        writer.putInt(0, i)
      writer.finish()
    }

    checkFilters(file,
      IntEquals(0, 3) -> Seq(3),
      IntEquals(0, 999) -> Nil,
      Not(IntEquals(0, 3)) -> Seq(1, 2, 4, 5, 6, 7, 8, 9, 10),
      IntGreaterThan(0, 7) -> Seq(8, 9, 10),
      IntLessThan(0, 3) -> Seq(1, 2),
      Or(IntEquals(0, 3), IntEquals(0, 7), IntEquals(0, 9)) -> Seq(3, 7, 9),
      And(IntGreaterThan(0, 3), IntLessThan(0, 7)) -> Seq(4, 5, 6))
  }

  it should "filter by long" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Long, "foo")
      for (i <- 1L to 10L)
        writer.putLong(0, i)
      writer.finish()
    }

    checkFilters(file,
      LongEquals(0, 3L) -> Seq(3L),
      LongEquals(0, 999L) -> Nil,
      Not(LongEquals(0, 3L)) -> Seq(1L, 2L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
      LongGreaterThan(0, 7L) -> Seq(8L, 9L, 10L),
      LongLessThan(0, 3L) -> Seq(1L, 2L),
      Or(LongEquals(0, 3L), LongEquals(0, 7L), LongEquals(0, 9L)) -> Seq(3L, 7L, 9L),
      And(LongGreaterThan(0, 3L), LongLessThan(0, 7L)) -> Seq(4L, 5L, 6L))
  }

  it should "filter by float" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Float, "foo")
      for (i <- Seq(1.1f, 3.3f, 2.2f, 4.4f, 5.5f, 6.6f, 7.7f, 8.8f, 9.9f, 10f))
        writer.putFloat(0, i)
      writer.finish()
    }

    checkFilters(file,
      FloatEquals(0, 3.3f) -> Seq(3.3f),
      FloatEquals(0, 999.9f) -> Nil,
      Not(FloatEquals(0, 3.3f)) -> Seq(1.1f, 2.2f, 4.4f, 5.5f, 6.6f, 7.7f, 8.8f, 9.9f, 10f),
      FloatGreaterThan(0, 7f) -> Seq(7.7f, 8.8f, 9.9f, 10f),
      FloatLessThan(0, 3f) -> Seq(1.1f, 2.2f),
      Or(FloatEquals(0, 3.3f), FloatEquals(0, 7.7f), FloatEquals(0, 9.9f)) -> Seq(3.3f, 7.7f, 9.9f),
      And(FloatGreaterThan(0, 4f), FloatLessThan(0, 7.7f)) -> Seq(4.4f, 5.5f, 6.6f))
  }

  it should "filter by double" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.Double, "foo")
      for (i <- Seq(1.1, 3.3, 2.2, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10))
        writer.putDouble(0, i)
      writer.finish()
    }

    checkFilters(file,
      DoubleEquals(0, 3.3) -> Seq(3.3),
      DoubleEquals(0, 999.9) -> Nil,
      Not(DoubleEquals(0, 3.3)) -> Seq(1.1, 2.2, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10),
      DoubleGreaterThan(0, 7) -> Seq(7.7, 8.8, 9.9, 10),
      DoubleLessThan(0, 3) -> Seq(1.1, 2.2),
      Or(DoubleEquals(0, 3.3), DoubleEquals(0, 7.7), DoubleEquals(0, 9.9)) -> Seq(3.3, 7.7, 9.9),
      And(DoubleGreaterThan(0, 4), DoubleLessThan(0, 7.7)) -> Seq(4.4, 5.5, 6.6))
  }

  it should "filter by string" in test { file =>
    withWriter(file) { writer =>
      writer.addColumn(ColumnType.String, "foo")
      for (str <- Seq("a", "b", "c", "foo", "FOO"))
        writer.putString(0, str)
      writer.finish()
    }

    checkFilters(file,
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
      StringContains(0, "o", StringLocation.Any) -> Seq("foo"))
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