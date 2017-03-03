package zcs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import zcs.jni.Reader

class ZCSRDD(sc: SparkContext,
             path: String,
             columns: Array[Int],
             schema: StructType,
             partitions: Array[Partition]) extends RDD[Row](sc, Nil) {

  def getPartitions: Array[Partition] = partitions

  def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val reader = new Reader(path)

    def close(): Unit = logInfo("Closing reader, probably!")

    context.addTaskCompletionListener(_ => close())

    RowIterator(context, reader, columns, schema)
  }
}
