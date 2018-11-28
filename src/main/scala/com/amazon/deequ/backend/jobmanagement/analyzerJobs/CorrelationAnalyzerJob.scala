package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{Correlation, CorrelationState}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class CorrelationAnalyzerParams(var context: String, var table: String,
                                var firstColumn: String, var secondColumn: String,
                                var where: Option[String] = None)

object CorrelationAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[CorrelationAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[CorrelationState, DoubleMetric, JdbcCorrelation](
        JdbcCorrelation(params.firstColumn, params.secondColumn, params.where), params.table)
      case "spark" => analyzerWithSpark[CorrelationState, DoubleMetric, Correlation](
        Correlation(params.firstColumn, params.secondColumn, params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
