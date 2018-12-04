package com.amazon.deequ.backend.servlets

import org.scalatra.{Ok, ScalatraServlet}
import com.amazon.deequ.backend.utils.JdbcUtils.withJdbc
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import org.slf4j.LoggerFactory

class DBAccessServlet extends ScalatraServlet
  with JacksonJsonSupport {
  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  private val logger =  LoggerFactory.getLogger(getClass)

  before() {
    contentType = formats("json")
  }

  get("/select-tables") {
    var tables = ""
    withJdbc { connection =>
        //show all table names in db
        var list = List[String]()
        val md = connection.getMetaData
        val rs = md.getTables(null, null, "%", Array[String]("TABLE"))
          while (rs.next){
            list = rs.getString(3) :: list
          }
        list = list.sorted
        tables = list.mkString(", ")
    }
    Ok("tables" -> tables)
  }
  get("/select-columns") {
    var columns = ""
    withJdbc { connection =>
      //show all distinct column names in db
      var set = Set[String]() //use set here to remove duplicates
      val md = connection.getMetaData
      val rs = md.getColumns(null, null, "%",null)
      while (rs.next){
        set += rs.getString(3)
      }
      val list = set.toList
      columns = list.sorted.mkString(", ")
    }
    Ok("cols" -> columns)

  }
}
