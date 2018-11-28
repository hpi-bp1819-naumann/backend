package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc.JdbcMaximum
import com.amazon.deequ.analyzers.{MaxState, Maximum}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class MaximumAnalyzerParams(var context: String, var table: String,
                            var column: String, var where: Option[String] = None)

object MaximumAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[MaximumAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[MaxState, DoubleMetric, JdbcMaximum](
        JdbcMaximum(params.column, params.where), params.table)
      case "spark" => analyzerWithSpark[MaxState, DoubleMetric, Maximum](
        Maximum(params.column, params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
