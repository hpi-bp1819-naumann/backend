package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{Sum, SumState}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class SumAnalyzerParams(var context: String, var table: String,
                        var column: String, var where: Option[String] = None)

object SumAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[SumAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[SumState, DoubleMetric, JdbcSum](
        JdbcSum(params.column, params.where), params.table)
      case "spark" => analyzerWithSpark[SumState, DoubleMetric, Sum](
        Sum(params.column, params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
