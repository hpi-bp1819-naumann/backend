package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{NumMatchesAndCount, PatternMatch}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, AnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

import scala.util.matching.Regex

case class PatternMatchAnalyzerParams(context: String, table: String,
                                 var column: String, var pattern: Regex,
                                 var where: Option[String] = None)
  extends AnalyzerParams

object PatternMatchAnalyzerJob extends AnalyzerJob[PatternMatchAnalyzerParams] {

  val name = "PatternMatch"
  val description = "description for patternMatch analyzer"

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[PatternMatchAnalyzerParams]

  def extractFromJson(requestParams: JValue): PatternMatchAnalyzerParams = {
    requestParams.extract[PatternMatchAnalyzerParams]
  }

  def funcWithJdbc(params: PatternMatchAnalyzerParams): Any = {
    analyzerWithJdbc[NumMatchesAndCount, DoubleMetric, JdbcPatternMatch](
      JdbcPatternMatch(params.column, params.pattern, params.where), params.table)
  }

  def funcWithSpark(params: PatternMatchAnalyzerParams) {
    analyzerWithSpark[NumMatchesAndCount, DoubleMetric, PatternMatch](
      PatternMatch(params.column, params.pattern, params.where), params.table)
  }
}
