package zcs.spark

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import zcs.jni.{ColumnType, Reader}

case class Relation(reader: Reader)
                   (@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan {

  def sqlContext: SQLContext = sparkSession.sqlContext

  val schema: StructType = {
    val fields = for {
      i <- 0 until reader.columnCount
      name = s"_$i"
      field = fieldType(reader.columnType(i))
    } yield StructField(name, field, nullable = true)

    StructType(fields)
  }

  val rowGroups: Array[Partition] = Array(RowGroup(0)) // FIXME

  private def fieldType(columnType: ColumnType) =
    columnType match {
      case ColumnType.Boolean => BooleanType
      case ColumnType.Int => IntegerType
      case ColumnType.Long => LongType
      case ColumnType.String => StringType
    }

  def buildScan: RDD[Row] =
    new ZCSRDD(sparkSession.sparkContext, reader, schema, rowGroups)
}
