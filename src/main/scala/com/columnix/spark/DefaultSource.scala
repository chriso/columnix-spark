package com.columnix.spark

import java.nio.file.{Paths, Files}

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources._

class DefaultSource extends RelationProvider
  with CreatableRelationProvider
  with DataSourceRegister {

  def shortName: String = "columnix"

  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String]): BaseRelation = {

    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    Relation(path, sqlContext)
  }

  def createRelation(sqlContext: SQLContext, mode: SaveMode,
                     parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))

    if (Files.exists(Paths.get(path))) {
      if (mode == SaveMode.Append)
        throw new UnsupportedOperationException("append to an existing file")
      else if (mode == SaveMode.ErrorIfExists)
        throw new RuntimeException("file exists")
      else if (mode == SaveMode.Ignore)
        return Relation(path, sqlContext)
    }

    // FIXME: configurable compression, encoding and row group size

    val writer = Writer(path, data)

    try writer.write()
    finally writer.close()

    Relation(path, sqlContext)
  }
}
