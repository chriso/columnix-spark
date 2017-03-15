package com.columnix.spark.partitioned

import com.columnix.spark.RowWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

class ColumnixWriterFactory(parameters: Map[String, String]) extends OutputWriterFactory {

  override def getFileExtension(context: TaskAttemptContext): String = ".cx"

  override def newInstance(rawPath: String,
                           dataSchema: StructType,
                           context: TaskAttemptContext): OutputWriter = {

    val path = new Path(rawPath)
    val fs = path.getFileSystem(new Configuration)
    if (fs.getScheme != "file")
      throw new UnsupportedOperationException(s"cannot write to path: $path")

    fs.mkdirs(path.getParent)

    RowWriter(path.toUri.getPath, dataSchema, parameters)
  }
}
