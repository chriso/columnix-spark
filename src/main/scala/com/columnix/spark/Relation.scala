package com.columnix.spark

import com.columnix.jni.{ColumnType, Reader}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

case class Relation(path: String)(@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan {

  val schema: StructType = inferSchema

  private[this] val columnIndexByName = schema.fields.map(_.name).zipWithIndex.toMap

  private[this] val dataTypes = schema.fields map (_.dataType)

  private[this] val filterTranslator = FilterTranslator(columnIndexByName, dataTypes)

  private def inferSchema: StructType = {
    val reader = new Reader(path)
    try {
      val fields = for {
        i <- 0 until reader.columnCount
        name = reader.columnName(i)
        fieldType = dataType(reader.columnType(i))
      } yield StructField(name, fieldType, nullable = true)

      StructType(fields)
    } finally reader.close()
  }

  private def dataType(columnType: ColumnType.ColumnType) =
    columnType match {
      case ColumnType.Boolean => BooleanType
      case ColumnType.Int => IntegerType
      case ColumnType.Long => LongType
      case ColumnType.String => StringType
    }

  def buildScan(requiredColumns: Array[String], pushedFilters: Array[Filter]): RDD[Row] = {

    val columns = requiredColumns map columnIndexByName
    val filter = filterTranslator.translateFilters(pushedFilters: _*)

    val rdd = new ColumnixRDD(sparkContext, path, columns, dataTypes, filter)
    rdd.asInstanceOf[RDD[Row]]
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = Array.empty

  override def needConversion: Boolean = false

  def sqlContext: SQLContext = sparkSession.sqlContext

  def sparkContext: SparkContext = sparkSession.sparkContext
}

object Relation {

  def apply(path: String, sqlContext: SQLContext): Relation =
    Relation(path)(sqlContext.sparkSession)
}
