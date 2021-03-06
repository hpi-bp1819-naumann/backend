package com.amazon.deequ.backend.dbAccess

import java.sql.ResultSet

import com.amazon.deequ.backend.utils.JdbcUtils.withJdbc
import com.amazon.deequ.backend.utils.NoSuchTableException
import org.slf4j.LoggerFactory

class DbAccess {

  private val logger = LoggerFactory.getLogger(getClass)

  def executeQuery(query: String): QueryResult = {
    var columns = Seq[String]()
    var data = Seq[Seq[String]]()
    withJdbc { connection =>
      val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery()
      val metaData = rs.getMetaData

      val columnCount = metaData.getColumnCount

      columns = (1 to columnCount).map(i => metaData.getColumnName(i))

      while (rs.next) {
        data ++= Seq[Seq[String]]((1 to columnCount).map(
          i => {
            val columnValue = Option(rs.getObject(i))
            columnValue match {
              case Some(theColumnValue) =>
                theColumnValue.toString
              case None =>
                "null"
          }
        }))
      }
    }
    QueryResult(columns, data)
  }

  def getTables(): List[String] = {
    var tables = List[String]()
    withJdbc { connection =>
      val md = connection.getMetaData
      val rs = md.getTables(null, null, null, Array[String]("TABLE"))
      while (rs.next) {
        tables = rs.getString(3) :: tables
      }
      tables = tables.sorted
    }
    tables
  }


  def getColumns(): List[String] = {
    var columns = List[String]()
    withJdbc { connection =>
      var set = Set[String]() //use set here to remove duplicates
      val md = connection.getMetaData
      val rs = md.getColumns(null, null, "%", null)
      while (rs.next) {
        set += rs.getString(4)
      }
      columns = set.toList.sorted
    }
    columns
  }


  def getSchemas(): List[String] = {
    var schemas = List[String]()
    withJdbc { connection =>
      val query =
        s"""
           |SELECT schema_name
           |FROM information_schema.schemata
      """.stripMargin
      val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery()
      while (rs.next) {
        schemas = rs.getString(1) :: schemas
      }
      schemas = schemas.sorted
    }
    schemas
  }


  def getMetaData(tableName: String): Map[String, Any] = {

    var metaData = Map[String, Any]()

    withJdbc { connection =>

      val query =
        s"""
           |SELECT
           | *
           |FROM
           | $tableName
           |LIMIT 0
      """.stripMargin

      val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)

      val result = statement.executeQuery()
      val col_count = result.getMetaData.getColumnCount

      metaData += ("table" -> tableName)
      metaData += ("columns" -> (1 to col_count).map(i =>
        Map("name" -> result.getMetaData.getColumnName(i),
          "dataType" -> result.getMetaData.getColumnTypeName(i))))
    }

    metaData
  }

  def getMetaDataForAllTables(): List[Map[String, Any]] = {
    var metadata = List[Map[String, Any]]()
    val tables = getTables()
    for (t <- tables) {
      metadata = getMetaData(t) :: metadata
    }
    metadata
  }

  def getTopNRows(tableName: String, n: Int = 10 ): Seq[Seq[String]] = {

    var rows = Seq[Seq[String]]()

    withJdbc { connection =>

      val query =
        s"""
           |SELECT
           | *
           |FROM
           | $tableName
           |LIMIT
           | $n
      """.stripMargin

      val statement = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY)

      val result = statement.executeQuery
      val col_count = result.getMetaData.getColumnCount

      while (result.next()) {
        rows ++= Seq[Seq[String]]((1 to col_count).map(i => result.getString(i)))
      }
    }

    rows
  }

  def getTableData(tableName: String, n: Int = 10): Map[String, Any] = {
    if (!getTables().contains(tableName)) {
      throw NoSuchTableException(s"Input data does not include table $tableName!")
    }
    val rows = Map("rows" -> getTopNRows(tableName, n))

    getMetaData(tableName) ++ rows
  }

  def getVersion(productName: String): String = {
    var version = ""
    withJdbc { connection =>
      val md = connection.getMetaData
      if(productName == "jdbc") {
        version = md.getDriverMajorVersion.toString + "." + md.getDriverMinorVersion.toString
      }
      else if(productName == "db") {
        version = md.getDatabaseProductName + " " + md.getDatabaseProductVersion
      }
      else {
        version = "Unknown product!"
      }
    }
    version
  }
}

case class Query(query: String)

case class QueryResult(columns: Seq[String], data: Seq[Seq[String]])
