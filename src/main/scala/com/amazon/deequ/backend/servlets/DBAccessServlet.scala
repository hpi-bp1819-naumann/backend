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


  get("/select-tables") {
    withJdbc { connection =>
          //show all table names in db
        var list = List[String]()
        val md = connection.getMetaData
        val rs = md.getTables(null, null, "%", Array[String]("TABLE"))
          while (rs.next){
            list = rs.getString(3) :: list
            logger.debug(rs.getString(3))
          }
        println(list.mkString(", "))
        Ok("result" -> list.mkString(", "))
      }
  }
}
