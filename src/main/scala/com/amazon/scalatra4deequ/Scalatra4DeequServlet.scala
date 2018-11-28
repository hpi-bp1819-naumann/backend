package com.amazon.scalatra4deequ

import org.scalatra.ScalatraServlet
import org.scalatra.scalate.ScalateSupport
import com.amazon.deequ.analyzers.jdbc._
import org.slf4j.{Logger, LoggerFactory}

object Server extends Scalatra4DeequServlet

class Scalatra4DeequServlet() extends ScalatraServlet with ScalateSupport {
    val logger =  LoggerFactory.getLogger(getClass)

    get("/") {
    contentType = "text/html"
    }
    post("/new") {
    //TODO
    redirect("/")
    }
    get("/completeness") {
      //JdbcCompleteness()
    }
    get("/compliance") {
      //JdbcCompliance()
    }
    get("/correlation") {
      //JdbcCorrelation()
    }
    get("/countDistinct") {
      //JdbcCountDistinct()
    }
    get("/dataType") {
      //JdbcDataType()
    }
    get("/distinctness") {
      //JdbcDistinctness()
    }
    get("/entropy") {
      EntropyWithJdbc
      redirect("/")
    }
    get("/histogram") {
      //JdbcHistogram()
    }
    get("/maximum") {
      //JdbcMaximum()
    }
    get("/mean") {
      //JdbcMean()
    }
    get("/minimum") {
      //JdbcMinimum()
    }
    get("/patternMatch") {
      //JdbcPatternMatch()
    }
    get("/size") {
      //JdbcSize()
    }
    get("/standardDeviation") {
      //JdbcStandardDeviation()
    }
    get("/sum") {
      logger.debug("Starting search for nearest gas station.")
      "hi1"
    }
    get("/uniqueness") {
      //JdbcUniqueness()
    }
}