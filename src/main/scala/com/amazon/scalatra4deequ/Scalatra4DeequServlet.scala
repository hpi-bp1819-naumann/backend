package com.amazon.scalatra4deequ

import org.scalatra.ScalatraServlet
import org.scalatra.scalate.ScalateSupport
import com.amazon.deequ.analyzers.jdbc._

class Scalatra4DeequServlet() extends ScalatraServlet with ScalateSupport {
    get("/") {
    contentType = "text/html"
    }
    post("/new") {
    //TODO
    redirect("/")
    }
    get("/completeness") {
      JdbcCompleteness()
    }
    get("/compliance") {
      JdbcCompliance()
    }
    get("/correlation") {
      JdbcCorrelation()
    }
    get("/countDistinct") {
      JdbcCountDistinct()
    }
    get("/dataType") {
      JdbcDataType()
    }
    get("/distinctness") {
      JdbcDistinctness()
    }
    get("/entropy") {
      JdbcEntropy()
    }
    get("/histogram") {
      JdbcHistogram()
    }
    get("/maximum") {
      JdbcMaximum()
    }
    get("/mean") {
      JdbcMean()
    }
    get("/minimum") {
      JdbcMinimum()
    }
    get("/patternMatch") {
      JdbcPatternMatch()
    }
    get("/size") {
      JdbcSize()
    }
    get("/standardDeviation") {
      JdbcStandardDeviation()
    }
    get("/sum") {
      JdbcSum()
    }
    get("/uniqueness") {
      JdbcUniqueness()
    }
}