package zcs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Partition, SparkContext, TaskContext, TaskKilledException}
import zcs.jni.Reader

class ZCSRDD(sc: SparkContext,
             reader: Reader,
             columns: Array[Int],
             schema: StructType,
             partitions: Array[Partition]) extends RDD[Row](sc, Nil) {

  def getPartitions: Array[Partition] = partitions

  def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    reader.rewind()

    def close(): Unit = logInfo("Closing reader, probably!")

    context.addTaskCompletionListener(_ => close())

    RowIterator(context, reader, columns, schema)
  }
}

case class RowIterator(context: TaskContext,
                       reader: Reader,
                       columns: Array[Int],
                       schema: StructType) extends Iterator[Row] {

  private[this] val fieldTypes = schema.fields.map(_.dataType)

  private[this] val indices = columns.zipWithIndex

  private[this] val mutableRow = new SpecificInternalRow(fieldTypes)

  private[this] val encoder = RowEncoder(schema).resolveAndBind()

  def next: Row = {
    for ((in, out) <- indices) {
      if (reader.isNull(in))
        mutableRow.setNullAt(out)
      else {
        fieldTypes(out) match {
          case BooleanType => mutableRow.setBoolean(out, reader.getBoolean(in))
          case IntegerType => mutableRow.setInt(out, reader.getInt(in))
          case LongType => mutableRow.setLong(out, reader.getLong(in))
          case StringType => mutableRow.update(out, UTF8String.fromString(reader.getString(in)))
        }
      }
    }

    encoder.fromRow(mutableRow)
  }

  def hasNext: Boolean = {
    if (context.isInterrupted)
      throw new TaskKilledException

    reader.next
  }
}
