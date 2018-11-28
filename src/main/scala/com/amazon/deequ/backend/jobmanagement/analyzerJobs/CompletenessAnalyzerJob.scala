package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc.JdbcCompleteness
import com.amazon.deequ.analyzers.{Completeness, NumMatchesAndCount}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class CompletenessAnalyzerParams(var context: String, var table: String,
                                 var column: String, var where: Option[String] = None)

object CompletenessAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[CompletenessAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[NumMatchesAndCount, DoubleMetric, JdbcCompleteness](
        JdbcCompleteness(params.column, params.where), params.table)
      case "spark" => analyzerWithSpark[NumMatchesAndCount, DoubleMetric, Completeness](
        Completeness(params.column, params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
