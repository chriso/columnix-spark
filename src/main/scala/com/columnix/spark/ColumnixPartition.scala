package com.columnix.spark

import org.apache.spark.Partition

case class ColumnixPartition(path: String, index: Int) extends Partition
