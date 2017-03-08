package zcs.spark

import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{sources => spark}
import org.scalatest.{FlatSpec, Matchers}
import zcs.jni._

class FilterTranslatorTest extends FlatSpec with Matchers {

  behavior of "FilterTranslator"

  private val translator = FilterTranslator(
    Map("bool" -> 0, "int" -> 1, "long" -> 2, "str" -> 3),
    IndexedSeq(BooleanType, IntegerType, LongType, StringType))

  it should "translate an array of spark filters" in {
    translator.translateFilters() shouldEqual None
    translator.translateFilters(spark.IsNull("int")) shouldEqual Some(IsNull(1))
    translator.translateFilters(spark.IsNull("int"), spark.IsNull("long")) shouldEqual Some(
      And(IsNull(1), IsNull(2)))
  }

  private def translate(filter: spark.Filter) =
    translator.translateFilters(filter).get

  it should "translate boolean filters" in {
    translate(spark.EqualTo("bool", false)) shouldEqual BooleanEquals(0, false)
    translate(spark.EqualTo("bool", true)) shouldEqual BooleanEquals(0, true)
    translate(spark.EqualNullSafe("bool", false)) shouldEqual And(IsNotNull(0), BooleanEquals(0, false))
  }

  it should "translate long filters" in {
    translate(spark.EqualTo("long", 10)) shouldEqual LongEquals(2, 10L)
    translate(spark.EqualTo("long", 10L)) shouldEqual LongEquals(2, 10L)
    translate(spark.EqualNullSafe("long", 10L)) shouldEqual And(IsNotNull(2), LongEquals(2, 10L))
    translate(spark.LessThan("long", 10)) shouldEqual LongLessThan(2, 10)
    translate(spark.LessThan("long", 10L)) shouldEqual LongLessThan(2, 10L)
    translate(spark.LessThanOrEqual("long", 10)) shouldEqual Not(LongGreaterThan(2, 10))
    translate(spark.LessThanOrEqual("long", 10L)) shouldEqual Not(LongGreaterThan(2, 10L))
    translate(spark.GreaterThan("long", 10)) shouldEqual LongGreaterThan(2, 10)
    translate(spark.GreaterThan("long", 10L)) shouldEqual LongGreaterThan(2, 10L)
    translate(spark.GreaterThanOrEqual("long", 10)) shouldEqual Not(LongLessThan(2, 10))
    translate(spark.GreaterThanOrEqual("long", 10L)) shouldEqual Not(LongLessThan(2, 10L))
    translate(spark.In("long", Array(1, 2L, 3))) shouldEqual Or(
      LongEquals(2, 1L),
      LongEquals(2, 2L),
      LongEquals(2, 3L))
  }

  it should "translate string filters" in {
    translate(spark.EqualTo("str", "foo")) shouldEqual StringEquals(3, "foo")
    translate(spark.EqualTo("str", 10)) shouldEqual StringEquals(3, "10")
    translate(spark.LessThan("str", "foo")) shouldEqual StringLessThan(3, "foo")
    translate(spark.LessThan("str", 10L)) shouldEqual StringLessThan(3, "10")
    translate(spark.GreaterThan("str", "foo")) shouldEqual StringGreaterThan(3, "foo")
    translate(spark.GreaterThan("str", 10)) shouldEqual StringGreaterThan(3, "10")
    translate(spark.LessThanOrEqual("str", "foo")) shouldEqual Not(StringGreaterThan(3, "foo"))
    translate(spark.GreaterThanOrEqual("str", "foo")) shouldEqual Not(StringLessThan(3, "foo"))
    translate(spark.StringStartsWith("str", "foo")) shouldEqual StringContains(3, "foo", StringLocation.Start)
    translate(spark.StringEndsWith("str", "foo")) shouldEqual StringContains(3, "foo", StringLocation.End)
    translate(spark.StringContains("str", "foo")) shouldEqual StringContains(3, "foo", StringLocation.Any)
    translate(spark.In("str", Array("foo", "bar", 10))) shouldEqual Or(
      StringEquals(3, "foo"),
      StringEquals(3, "bar"),
      StringEquals(3, "10"))
  }

}
