package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{FrequenciesAndNumRows, UniqueValueRatio}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class UniqueValueRatioAnalyzerParams(var context: String, var table: String,
                                     var column: String)

object UniqueValueRatioAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[UniquenessAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcUniqueValueRatio](
        JdbcUniqueValueRatio(params.column), params.table)
      case "spark" => analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, UniqueValueRatio](
        UniqueValueRatio(params.column), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
