package zcs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import zcs.jni.{Reader, Filter}

class ZCSRDD(sc: SparkContext,
             path: String,
             columns: Array[Int],
             schema: StructType,
             filter: Option[Filter],
             partitions: Array[Partition]) extends RDD[InternalRow](sc, Nil) {

  def getPartitions: Array[Partition] = partitions

  def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    val reader = new Reader(path, filter)

    context.addTaskCompletionListener(_ => reader.close())
    context.addTaskFailureListener((_, _) => reader.close())

    if (columns.isEmpty)
      EmptySchemaIterator(context, reader)
    else
      RowIterator(context, reader, columns, schema)
  }
}
