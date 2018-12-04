package com.amazon.deequ.backend.servlets

import org.scalatra.{FutureSupport, ScalatraServlet}
import slick.jdbc.PostgresProfile.api._

class SlickServlet(val db: Database) extends ScalatraServlet with FutureSupport{

  get("/db/create-tables") {
    //create new table in db
    db.run()
  }

  get("/db/load-data") {
    //insert data into db
    db.run()
  }

  get("/db/select-tables") {
    //show all table names in db
  }
  get("/db/drop-tables") {
    //drop all tables in db
    db.run()
  }
}
