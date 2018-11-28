package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc.JdbcMinimum
import com.amazon.deequ.analyzers.{MinState, Minimum}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class MinimumAnalyzerParams(var context: String, var table: String,
                            var column: String, var where: Option[String] = None)

object MinimumAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[MaximumAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[MinState, DoubleMetric, JdbcMinimum](
        JdbcMinimum(params.column, params.where), params.table)
      case "spark" => analyzerWithSpark[MinState, DoubleMetric, Minimum](
        Minimum(params.column, params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
