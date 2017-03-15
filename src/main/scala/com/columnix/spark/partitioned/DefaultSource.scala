package com.columnix.spark.partitioned

import com.columnix.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

class DefaultSource extends FileFormat with DataSourceRegister {

  def shortName: String = "columnix"

  override def toString: String = "columnix"

  def inferSchema(sparkSession: SparkSession,
                  options: Map[String, String],
                  files: Seq[FileStatus]): Option[StructType] = {

    val path = files.head.getPath
    val fs = path.getFileSystem(new Configuration)
    if (fs.getScheme != "file")
      throw new UnsupportedOperationException(s"cannot read from path: $path")

    Some(SchemaReader.read(path.toUri.getPath))
  }

  def prepareWrite(sparkSession: SparkSession,
                   job: Job,
                   parameters: Map[String, String],
                   dataSchema: StructType): OutputWriterFactory =
    new ColumnixWriterFactory(parameters)

  override def buildReader(sparkSession: SparkSession,
                           dataSchema: StructType,
                           partitionSchema: StructType,
                           requiredSchema: StructType,
                           filters: Seq[Filter],
                           options: Map[String, String],
                           hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val columnIndices = dataSchema.fields.map(_.name).zipWithIndex.toMap
    val dataTypes = dataSchema.fields.map(_.dataType)
    val requiredColumns = requiredSchema.fields.map(field => columnIndices(field.name))

    val filterTranslator = FilterTranslator(columnIndices, dataTypes)
    val filter = filterTranslator.translateFilters(filters: _*)

    (file: PartitionedFile) => {

      val path = new Path(file.filePath)
      val fs = path.getFileSystem(new Configuration)
      if (fs.getScheme != "file")
        throw new UnsupportedOperationException(s"cannot read from path: $path")

      RowIterator.build(TaskContext.get(), path.toUri.getPath,
        filter, requiredColumns, dataTypes)
    }
  }
}
