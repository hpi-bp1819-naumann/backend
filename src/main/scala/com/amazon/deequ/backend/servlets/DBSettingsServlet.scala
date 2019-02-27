package com.amazon.deequ.backend.servlets

import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization
import org.scalatra.Ok
import com.amazon.deequ.backend.dbSettings.DbSettings

class DBSettingsServlet extends Servlet {

  get("/uri") {
    val uri = DbSettings.dburi
    Ok(("uri" -> uri) ~ Nil)
  }

  post("/uri") {
    try {
      val params = parsedBody.extract[Map[String, String]]
      val uri = params("uri")
      DbSettings.dburi(uri)
      val response = "message" -> "Successfully changed URI"
      Ok(response)
    } catch errorHandling
  }

  get("/user"){
    val user = DbSettings.dbuser
    Ok(("user"-> user) ~ Nil)
  }

  post("/user") {
    try {
      val params = parsedBody.extract[Map[String, String]]
      val user = params("user")
      DbSettings.dbuser(user)
      val response = "message" -> "Successfully changed Username"
      Ok(response)
    } catch errorHandling
  }

  post("/password") {
    try {
      val params = parsedBody.extract[Map[String, String]]
      val pass = params("password")
      DbSettings.dbpass(pass)
      val response = "message" -> "Successfully changed Password"
      Ok(response)
    } catch errorHandling
  }
}
