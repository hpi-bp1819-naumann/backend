package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{Mean, MeanState}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class MeanAnalyzerParams(var context: String, var table: String,
                         var column: String, var where: Option[String] = None)

object MeanAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[MeanAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[MeanState, DoubleMetric, JdbcMean](
        JdbcMean(params.column, params.where), params.table)
      case "spark" => analyzerWithSpark[MeanState, DoubleMetric, Mean](
        Mean(params.column, params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
