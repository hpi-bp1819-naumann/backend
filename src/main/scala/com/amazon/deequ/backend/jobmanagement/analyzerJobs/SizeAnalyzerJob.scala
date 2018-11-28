package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{NumMatches, Size}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class SizeAnalyzerParams(var context: String, var table: String,
                         var where: Option[String] = None)

object SizeAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[SizeAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[NumMatches, DoubleMetric, JdbcSize](
        JdbcSize(params.where), params.table)
      case "spark" => analyzerWithSpark[NumMatches, DoubleMetric, Size](
        Size(params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
