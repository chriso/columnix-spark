package com.columnix.spark

import com.columnix.file._
import com.columnix.jni.StringLocation
import org.apache.spark.sql.types._
import org.apache.spark.sql.{sources => spark}
import org.scalatest.{FlatSpec, Matchers}

class FilterTranslatorTest extends FlatSpec with Matchers {

  behavior of "FilterTranslator"

  private val translator = FilterTranslator(
    Map("bool" -> 0, "int" -> 1, "long" -> 2, "float" -> 3, "double" -> 4, "str" -> 5),
    Array(BooleanType, IntegerType, LongType, FloatType, DoubleType, StringType))

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

  it should "translate int filters" in {
    translate(spark.EqualTo("int", 10)) shouldEqual IntEquals(1, 10)
    translate(spark.EqualNullSafe("int", 10)) shouldEqual And(IsNotNull(1), IntEquals(1, 10))
    translate(spark.LessThan("int", 10)) shouldEqual IntLessThan(1, 10)
    translate(spark.LessThanOrEqual("int", 10)) shouldEqual Not(IntGreaterThan(1, 10))
    translate(spark.GreaterThan("int", 10)) shouldEqual IntGreaterThan(1, 10)
    translate(spark.GreaterThanOrEqual("int", 10)) shouldEqual Not(IntLessThan(1, 10))
    translate(spark.In("int", Array(1, 2, 3))) shouldEqual Or(
      IntEquals(1, 1),
      IntEquals(1, 2),
      IntEquals(1, 3))
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

  it should "translate float filters" in {
    translate(spark.EqualTo("float", 10f)) shouldEqual FloatEquals(3, 10f)
    translate(spark.EqualNullSafe("float", 10f)) shouldEqual And(IsNotNull(3), FloatEquals(3, 10f))
    translate(spark.LessThan("float", 10f)) shouldEqual FloatLessThan(3, 10f)
    translate(spark.LessThanOrEqual("float", 10f)) shouldEqual Not(FloatGreaterThan(3, 10f))
    translate(spark.GreaterThan("float", 10f)) shouldEqual FloatGreaterThan(3, 10f)
    translate(spark.GreaterThanOrEqual("float", 10f)) shouldEqual Not(FloatLessThan(3, 10f))
    translate(spark.In("float", Array(1f, 2f, 3f))) shouldEqual Or(
      FloatEquals(3, 1f),
      FloatEquals(3, 2f),
      FloatEquals(3, 3f))
  }

  it should "translate double filters" in {
    translate(spark.EqualTo("double", 10.0)) shouldEqual DoubleEquals(4, 10.0)
    translate(spark.EqualNullSafe("double", 10.0)) shouldEqual And(IsNotNull(4), DoubleEquals(4, 10.0))
    translate(spark.LessThan("double", 10.0)) shouldEqual DoubleLessThan(4, 10.0)
    translate(spark.LessThanOrEqual("double", 10.0)) shouldEqual Not(DoubleGreaterThan(4, 10.0))
    translate(spark.GreaterThan("double", 10.0)) shouldEqual DoubleGreaterThan(4, 10.0)
    translate(spark.GreaterThanOrEqual("double", 10.0)) shouldEqual Not(DoubleLessThan(4, 10.0))
    translate(spark.In("double", Array(1.1, 2.2, 3.3))) shouldEqual Or(
      DoubleEquals(4, 1.1),
      DoubleEquals(4, 2.2),
      DoubleEquals(4, 3.3))
  }

  it should "translate string filters" in {
    translate(spark.EqualTo("str", "foo")) shouldEqual StringEquals(5, "foo")
    translate(spark.EqualTo("str", 10)) shouldEqual StringEquals(5, "10")
    translate(spark.LessThan("str", "foo")) shouldEqual StringLessThan(5, "foo")
    translate(spark.LessThan("str", 10L)) shouldEqual StringLessThan(5, "10")
    translate(spark.GreaterThan("str", "foo")) shouldEqual StringGreaterThan(5, "foo")
    translate(spark.GreaterThan("str", 10)) shouldEqual StringGreaterThan(5, "10")
    translate(spark.LessThanOrEqual("str", "foo")) shouldEqual Not(StringGreaterThan(5, "foo"))
    translate(spark.GreaterThanOrEqual("str", "foo")) shouldEqual Not(StringLessThan(5, "foo"))
    translate(spark.StringStartsWith("str", "foo")) shouldEqual StringContains(5, "foo", StringLocation.Start)
    translate(spark.StringEndsWith("str", "foo")) shouldEqual StringContains(5, "foo", StringLocation.End)
    translate(spark.StringContains("str", "foo")) shouldEqual StringContains(5, "foo", StringLocation.Any)
    translate(spark.In("str", Array("foo", "bar", 10))) shouldEqual Or(
      StringEquals(5, "foo"),
      StringEquals(5, "bar"),
      StringEquals(5, "10"))
  }

}
