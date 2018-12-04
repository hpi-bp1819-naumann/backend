package com.amazon.deequ.backend.dbAccess

import com.amazon.deequ.backend.utils.JdbcUtils.withJdbc

class DbAccess {

  def getTables(): List[String] = {
    withJdbc { connection =>
      var list = List[String]()
      val md = connection.getMetaData
      val rs = md.getTables(null, null, null, Array[String]("TABLE"))
      while (rs.next) {
        list = rs.getString(3) :: list
      }
      return list
    }
    List[String]()
  }
}
