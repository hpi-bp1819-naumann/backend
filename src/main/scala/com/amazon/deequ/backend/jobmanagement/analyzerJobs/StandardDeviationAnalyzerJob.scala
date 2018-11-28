package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{StandardDeviation, StandardDeviationState}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class StandardDeviationAnalyzerParams(var context: String, var table: String,
                                      var column: String, var where: Option[String] = None)

object StandardDeviationAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[StandardDeviationAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[StandardDeviationState, DoubleMetric, JdbcStandardDeviation](
        JdbcStandardDeviation(params.column, params.where), params.table)
      case "spark" => analyzerWithSpark[StandardDeviationState, DoubleMetric, StandardDeviation](
        StandardDeviation(params.column, params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
