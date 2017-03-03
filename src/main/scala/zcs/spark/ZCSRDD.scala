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
             schema: StructType,
             partitions: Array[Partition]) extends RDD[Row](sc, Nil) {

  def getPartitions: Array[Partition] = partitions

  def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    reader.rewind()

    def close(): Unit = logInfo("Closing reader, probably!")

    context.addTaskCompletionListener(_ => close())

    RowIterator(context, reader, schema: StructType)
  }
}

case class RowIterator(context: TaskContext, reader: Reader, schema: StructType) extends Iterator[Row] {

  private[this] val fieldTypes = schema.fields.map(_.dataType)
  private[this] val indices = fieldTypes.indices.toArray
  private[this] val mutableRow = new SpecificInternalRow(fieldTypes)

  private[this] val encoder = RowEncoder(schema).resolveAndBind()

  def next: Row = {
    for (i <- indices) {
      if (reader.isNull(i))
        mutableRow.setNullAt(i)
      else {
        fieldTypes(i) match {
          case BooleanType => mutableRow.setBoolean(i, reader.getBoolean(i))
          case IntegerType => mutableRow.setInt(i, reader.getInt(i))
          case LongType => mutableRow.setLong(i, reader.getLong(i))
          case StringType => mutableRow.update(i, UTF8String.fromString(reader.getString(i)))
        }
      }
    }

    encoder.fromRow(mutableRow)
  }

  def hasNext: Boolean = {
    if (context.isInterrupted)
      throw new TaskKilledException

    // FIXME: decref once this is false, in a finally block
    reader.next
  }
}
