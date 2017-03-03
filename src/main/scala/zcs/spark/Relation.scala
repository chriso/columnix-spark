package zcs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{Partition, SparkContext}
import zcs.jni.{ColumnType, Reader}

case class Relation(reader: Reader)
                   (@transient val sparkSession: SparkSession)
  extends BaseRelation with PrunedScan {

  def sqlContext: SQLContext = sparkSession.sqlContext

  def sparkContext: SparkContext = sparkSession.sparkContext

  val schema: StructType = {
    val fields = for {
      i <- 0 until reader.columnCount
      name = s"_$i"
      field = fieldType(reader.columnType(i))
    } yield StructField(name, field, nullable = true)

    StructType(fields)
  }

  val columnByName = schema.fields.map(_.name).zipWithIndex.toMap

  val fieldsByIndex = schema.fields.toIndexedSeq

  val rowGroups: Array[Partition] = Array(RowGroup(0)) // FIXME

  private def fieldType(columnType: ColumnType) =
    columnType match {
      case ColumnType.Boolean => BooleanType
      case ColumnType.Int => IntegerType
      case ColumnType.Long => LongType
      case ColumnType.String => StringType
    }

  def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    val scanColumns = requiredColumns map columnByName
    val scanSchema = StructType(scanColumns map fieldsByIndex)
    new ZCSRDD(sparkContext, reader, scanColumns, scanSchema, rowGroups)
  }
}
