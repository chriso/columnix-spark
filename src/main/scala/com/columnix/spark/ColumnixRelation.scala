package com.columnix.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

case class ColumnixRelation(path: String,
                            knownSchema: Option[StructType] = None)
                           (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  val schema: StructType = knownSchema getOrElse SchemaReader.read(path)

  private[this] val columnIndexByName = schema.fields.map(_.name).zipWithIndex.toMap

  private[this] val dataTypes = schema.fields map (_.dataType)

  private[this] val filterTranslator = FilterTranslator(columnIndexByName, dataTypes)

  def buildScan(requiredColumns: Array[String],
                pushedFilters: Array[Filter]): RDD[Row] = {

    val columns = requiredColumns map columnIndexByName
    val filter = filterTranslator.translateFilters(pushedFilters: _*)
    val rdd = new ColumnixRDD(sparkContext, path, columns, dataTypes, filter)
    rdd.asInstanceOf[RDD[Row]]
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = Array.empty

  override def needConversion: Boolean = false

  private def sparkContext: SparkContext = sqlContext.sparkSession.sparkContext
}
