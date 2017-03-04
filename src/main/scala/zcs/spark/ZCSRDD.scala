package zcs.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.{Partition, SparkContext, TaskContext}
import zcs.jni.Reader
import zcs.jni.predicates.Predicate

class ZCSRDD(sc: SparkContext,
             path: String,
             columns: Array[Int],
             schema: StructType,
             filters: Array[Filter],
             partitions: Array[Partition]) extends RDD[InternalRow](sc, Nil) {

  def getPartitions: Array[Partition] = partitions

  def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    // FIXME: get this dynamically
    val predicate = Some(new zcs.jni.predicates.Equals(0, 1384203753000L))

    val reader = predicate match {
      case Some(pred) => new Reader(path, pred)
      case _ => new Reader(path)
    }

    def close(): Unit = logInfo("Closing reader, probably!")

    context.addTaskCompletionListener(_ => close())

    RowIterator(context, reader, columns, schema)
  }
}
