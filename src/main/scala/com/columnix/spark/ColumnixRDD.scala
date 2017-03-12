package com.columnix.spark

import com.columnix.jni.{Filter, Reader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}

class ColumnixRDD(sc: SparkContext,
                  path: String,
                  columns: Array[Int],
                  dataTypes: Array[DataType],
                  filter: Option[Filter]) extends RDD[InternalRow](sc, Nil) {

  def getPartitions: Array[Partition] = Array(ColumnixPartition(path, 0))

  def compute(partition: Partition, context: TaskContext): Iterator[InternalRow] = {

    val reader = new Reader(path, filter)

    context.addTaskCompletionListener(_ => reader.close())
    context.addTaskFailureListener((_, _) => reader.close())

    if (columns.isEmpty) CountIterator(reader)
    else RowIterator(context, reader, columns, dataTypes)
  }
}
