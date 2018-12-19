package com.amazon.deequ.backend.servlets

import com.amazon.deequ.backend.dbAccess.DbAccess
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization
import org.scalatra.Ok

class DBAccessServlet extends Servlet {

  val dbAccess = new DbAccess

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

  get ("/data/:table") {
    val tableName = params("table")

    try {
      val result = Serialization.write(dbAccess.getTableData(tableName))
      Ok(result, headers = Map[String, String]("content-Type" -> "application/json"))
    } catch errorHandling
  }
}
