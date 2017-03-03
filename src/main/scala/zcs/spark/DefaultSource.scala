package zcs.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class DefaultSource extends RelationProvider with DataSourceRegister {

  def shortName: String = "zcs"

  def createRelation(sqlContext: SQLContext,
                     parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    Relation(path)(sqlContext.sparkSession)
  }
}
