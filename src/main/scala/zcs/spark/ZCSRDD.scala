package zcs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import zcs.jni.{And, LongEquals, Or, Reader}

class ZCSRDD(sc: SparkContext,
             path: String,
             columns: Array[Int],
             schema: StructType,
             filters: Array[Filter],
             partitions: Array[Partition]) extends RDD[InternalRow](sc, Nil) {

  def getPartitions: Array[Partition] = partitions

  def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    // FIXME: get this dynamically
    val predicate = Some(Or(
      LongEquals(0, 1384203753000L),
      LongEquals(0, 1384699635000L)
    ))

    val reader = new Reader(path, predicate)

    def close(): Unit = logInfo("Closing reader, probably!")

    context.addTaskCompletionListener(_ => close())

    RowIterator(context, reader, columns, schema)
  }
}
