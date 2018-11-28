package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{NumMatchesAndCount, PatternMatch}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.DoubleMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

import scala.util.matching.Regex

class PatternMatchAnalyzerParams(var context: String, var table: String,
                                 var column: String, var pattern: Regex,
                                 var where: Option[String] = None)

object PatternMatchAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[PatternMatchAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[NumMatchesAndCount, DoubleMetric, JdbcPatternMatch](
        JdbcPatternMatch(params.column, params.pattern, params.where), params.table)
      case "spark" => analyzerWithSpark[NumMatchesAndCount, DoubleMetric, PatternMatch](
        PatternMatch(params.column, params.pattern, params.where), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}
