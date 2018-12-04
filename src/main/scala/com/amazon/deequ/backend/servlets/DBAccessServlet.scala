package com.amazon.deequ.backend.servlets

import com.amazon.deequ.backend.dbAccess.DbAccess
import org.scalatra.{Ok, ScalatraServlet}
import org.json4s.JsonDSL._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._
import org.slf4j.LoggerFactory

class DBAccessServlet extends ScalatraServlet
  with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats
  private val logger =  LoggerFactory.getLogger(getClass)

  val dbAccess = new DbAccess

  get("/tables") {
    val result = dbAccess.getTables()
    val response = ("tables" -> result) ~ Nil
    Ok(response)
  }
}
