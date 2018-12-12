package com.amazon.deequ.backend.dbAccess

import com.amazon.deequ.backend.utils.JdbcUtils.withJdbc
import org.scalatra.Ok

class DbAccess {


  def getTables() {
    var tables = ""
    withJdbc { connection =>
      var list = List[String]()
      val md = connection.getMetaData
      val rs = md.getTables(null, null, null, Array[String]("TABLE"))
      while (rs.next) {
        list = rs.getString(3) :: list
      }
      tables = list.sorted.mkString(", ")
    }
    Ok("tables" -> tables)
  }
  def getColumns() {
    var columns = ""
    withJdbc { connection =>
      var set = Set[String]() //use set here to remove duplicates
      val md = connection.getMetaData
      val rs = md.getColumns(null, null, "%",null)
      while (rs.next){
        set += rs.getString(3)
      }
      val list = set.toList
      columns = list.sorted.mkString(", ")
    }
    Ok("columns" -> columns)
  }
}
