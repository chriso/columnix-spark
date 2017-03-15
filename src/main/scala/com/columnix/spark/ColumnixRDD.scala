package com.columnix.spark

import com.columnix.file.Filter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.{Partition, SparkContext, TaskContext}

class ColumnixRDD(sc: SparkContext,
                  path: String,
                  columns: Array[Int],
                  dataTypes: Array[DataType],
                  filter: Option[Filter]) extends RDD[InternalRow](sc, Nil) {

  def getPartitions: Array[Partition] =
    Array(ColumnixPartition(path, 0))

  def compute(partition: Partition,
              context: TaskContext): Iterator[InternalRow] =
    RowIterator.build(context, path, filter, columns, dataTypes)
}
