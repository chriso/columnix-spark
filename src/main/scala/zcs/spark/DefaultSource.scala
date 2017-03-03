package zcs.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}
import zcs.jni.Reader

class DefaultSource extends RelationProvider with DataSourceRegister {

  def shortName: String = "zcs"

  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val reader = new Reader(path)
    Relation(reader)(sqlContext.sparkSession)
  }
}
