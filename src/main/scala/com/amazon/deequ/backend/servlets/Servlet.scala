package com.amazon.deequ.backend.servlets

import org.json4s.JsonDSL._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.{ActionResult, BadRequest, ScalatraServlet}
import org.slf4j.{Logger, LoggerFactory}

abstract class Servlet extends ScalatraServlet
  with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats
  protected val logger:Logger = LoggerFactory.getLogger(getClass)

  def generateErrorResponse(ex: Exception): ActionResult = {
    val response = ("message" -> "There was an error during processing your request") ~
      ("error" -> ("type" -> ex.getClass.toString) ~ ("message" -> ex.getMessage))
    BadRequest(response)
  }

  val errorHandling: PartialFunction[Throwable, ActionResult] = {
    case e: Exception => generateErrorResponse(e)
  }

  before() {
    contentType = formats("json")
  }
}
