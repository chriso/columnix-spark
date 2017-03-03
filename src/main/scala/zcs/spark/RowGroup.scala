package zcs.spark

import org.apache.spark.Partition

case class RowGroup(index: Int) extends Partition
