package com.amazon.deequ.backend.servlets

import com.amazon.deequ.backend.dbAccess.{DbAccess, Query}
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization
import org.scalatra.Ok

class DBAccessServlet extends Servlet {

  val dbAccess = new DbAccess

  post("/query") {
    try {
      val query = parsedBody.extract[Query]
      val result = dbAccess.executeQuery(query.query)
      val response = "result" -> result
      Ok(response)
    } catch errorHandling
  }

  get("/tables") {
    val tables = dbAccess.getTables()
    Ok(("tables" -> tables) ~ Nil)
  }

  get("/columns") {
    val columns = dbAccess.getColumns()
    Ok(("columns" -> columns) ~ Nil)
  }

  get("/schemas") {
    val schemas = dbAccess.getSchemas()
    Ok(("schemas" -> schemas) ~ Nil)
  }

  get("/data") {
    val metadata = dbAccess.getMetaDataForAllTables()
    Ok(metadata)
  }

  get("/data/:table") {
    val tableName = params("table")

    try {
      val result = Serialization.write(dbAccess.getTableData(tableName))
      Ok(result, headers = Map[String, String]("content-Type" -> "application/json"))
    } catch errorHandling
  }

  get("/rows/:table") {
    val tableName = params("table")

    val firstRows = dbAccess.getTopNRows(tableName)
    Ok(firstRows)
  }

  get("/rows/:table/:n") {
    val tableName = params("table")
    val number = params("n").toInt

    val firstRows = dbAccess.getTopNRows(tableName,number)
    Ok(firstRows)
  }

  get("/version/:product") {
    val productName = params("product")
    val version = dbAccess.getVersion(productName)
    Ok(productName -> version)
  }
}
