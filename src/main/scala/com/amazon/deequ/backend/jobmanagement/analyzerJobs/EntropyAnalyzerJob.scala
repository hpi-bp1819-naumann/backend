package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{Entropy, FrequenciesAndNumRows}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class EntropyAnalyzerParams(var context: String, var table: String,
                            var column: String)

object EntropyAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[EntropyAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcEntropy](
        JdbcEntropy(params.column), params.table)
      case "spark" => analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, Entropy](
        Entropy(params.column), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
