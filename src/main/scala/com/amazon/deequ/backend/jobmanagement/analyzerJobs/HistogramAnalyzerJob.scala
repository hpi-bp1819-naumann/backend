package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{FrequenciesAndNumRows, Histogram}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.HistogramMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class HistogramAnalyzerParams(var context: String, var table: String,
                              var column: String)

object HistogramAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[HistogramAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[JdbcFrequenciesAndNumRows, HistogramMetric, JdbcHistogram](
        JdbcHistogram(params.column), params.table)
      case "spark" => analyzerWithSpark[FrequenciesAndNumRows, HistogramMetric, Histogram](
        Histogram(params.column), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
