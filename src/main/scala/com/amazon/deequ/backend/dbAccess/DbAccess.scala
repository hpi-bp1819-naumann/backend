package com.amazon.deequ.backend.dbAccess

import java.sql.ResultSet

import com.amazon.deequ.backend.utils.JdbcUtils.withJdbc

class DbAccess {

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
      while (rs.next){
        schemas = rs.getString(1) :: schemas
      }
      schemas = schemas.sorted
    }
    schemas
  }
}
