package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class DistinctnessAnalyzerParams(var context: String, var table: String,
                                 var column: String)

object DistinctnessAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[DistinctnessAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcDistinctness](
        JdbcDistinctness(params.column), params.table)
      case "spark" => analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, Distinctness](
        Distinctness(params.column), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
