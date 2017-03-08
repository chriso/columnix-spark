package zcs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{Partition, SparkContext}
import zcs.jni.{ColumnType, Reader}

case class Relation(path: String)
                   (@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedFilteredScan {

  def sqlContext: SQLContext = sparkSession.sqlContext

  def sparkContext: SparkContext = sparkSession.sparkContext

  override val needConversion: Boolean = false

  val schema: StructType = inferSchema

  private def inferSchema: StructType = {
    val reader = new Reader(path)
    try {
      val fields = for {
        i <- 0 until reader.columnCount
        name = s"_$i"
        field = fieldType(reader.columnType(i))
      } yield StructField(name, field, nullable = true)

      StructType(fields)
    } finally reader.close()
  }

  private[this] val columnIndexByName = schema.fields.map(_.name).zipWithIndex.toMap

  private[this] val fieldsByColumnIndex = schema.fields.toIndexedSeq

  val rowGroups: Array[Partition] = Array(RowGroup(0)) // FIXME

  private def fieldType(columnType: ColumnType.ColumnType) =
    columnType match {
      case ColumnType.Boolean => BooleanType
      case ColumnType.Int => IntegerType
      case ColumnType.Long => LongType
      case ColumnType.String => StringType
    }

  def buildScan(requiredColumns: Array[String], pushedFilters: Array[Filter]): RDD[Row] = {
    val columns = requiredColumns map columnIndexByName
    val fields = columns map fieldsByColumnIndex
    val schema = StructType(fields)

    val translator = FilterTranslator(columnIndexByName, fields.map(_.dataType))
    val filter = translator.translateFilters(pushedFilters)

    val rdd = new ZCSRDD(sparkContext, path, columns, schema, filter, rowGroups)
    rdd.asInstanceOf[RDD[Row]]
  }
}
