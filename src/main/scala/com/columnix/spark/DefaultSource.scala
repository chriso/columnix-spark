package com.columnix.spark

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.sources._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider with CreatableRelationProvider
  with DataSourceRegister {

  def shortName: String = "columnix"

  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String]): BaseRelation = {

    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    ColumnixRelation(path, sqlContext)
  }

  def createRelation(sqlContext: SQLContext,
                     mode: SaveMode,
                     parameters: Map[String, String],
                     data: DataFrame): BaseRelation = {

    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))

    if (Files.exists(Paths.get(path))) {
      if (mode == SaveMode.Append)
        throw new UnsupportedOperationException("append to an existing file")
      else if (mode == SaveMode.ErrorIfExists)
        throw new RuntimeException("file exists")
      else if (mode == SaveMode.Ignore)
        return ColumnixRelation(path, sqlContext)
    }

    val writer = RowWriter(path, data.schema, parameters)

    try data foreach writer.write _
    finally writer.close()

    ColumnixRelation(path, sqlContext)
  }
}
