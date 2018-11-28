package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{Compliance, NumMatchesAndCount}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class ComplianceAnalyzerParams(var context: String, var table: String,
                               var instance: String, var predicate: String,
                               var where: Option[String] = None)

object ComplianceAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[ComplianceAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[NumMatchesAndCount, DoubleMetric, JdbcCompliance](
        JdbcCompliance(params.instance, params.predicate, params.where), params.table)
      case "spark" => analyzerWithSpark[NumMatchesAndCount, DoubleMetric, Compliance](
        Compliance(params.instance, params.predicate, params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
