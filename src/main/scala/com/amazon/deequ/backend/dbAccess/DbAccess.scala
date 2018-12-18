package com.amazon.deequ.backend.dbAccess

import java.sql.ResultSet

import com.amazon.deequ.backend.utils.JdbcUtils.withJdbc
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class DbAccess {

  private val logger = LoggerFactory.getLogger(getClass)

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
      val rs = md.getColumns(null, null, "%",null)
      while (rs.next){
        set += rs.getString(3)
      }
      columns = set.toList.sorted
    }
    columns
  }


  def getSchemas(): List[String] = {
    var schemas = List[String]()
    withJdbc { connection =>
      var set = Set[String]() //use set here to remove duplicates
      val md = connection.getMetaData
      val rs = md.getColumns(null, "%", null,null)
      while (rs.next){
        set += rs.getString(3)
      }
      schemas = set.toList.sorted
    }
    schemas
  }

  def getMetaData(tableName: String): Map[String, Seq[String]] = {

    var metaData = Map[String, Seq[String]]()

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
      var col_count = result.getMetaData.getColumnCount

      metaData += ("table" -> Seq[String](tableName))
      metaData += ("columnCount" -> Seq[String](col_count.toString))
      metaData += ("columns" -> (1 to col_count).map(i => result.getMetaData.getColumnName(i)))
    }

    metaData
  }

  def getTopNRows(tableName: String, n: Int): Seq[Seq[String]] = {

    var rows: Seq[Seq[String]] = Nil

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
}
