package com.columnix.spark

import org.apache.spark.Partition

case class RowGroup(index: Int) extends Partition
