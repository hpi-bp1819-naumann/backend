package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.PatternMatch
import com.amazon.deequ.analyzers.jdbc.JdbcPatternMatch
import com.amazon.deequ.backend.jobmanagement.{AnalyzerParams, RequestParameter}
import org.json4s.JValue

import scala.util.matching.Regex

case class PatternMatchAnalyzerParams(analyzer: String,
                                      column: String,
                                      pattern: Regex,
                                      where: Option[String] = None)
  extends AnalyzerParams

object PatternMatchAnalyzerExtractor extends AnalyzerExtractor[PatternMatchAnalyzerParams] {
  val name = "PatternMatch"
  val description = "Gives the fraction of values that match a certain regex constraint divided by all values in the given column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[PatternMatchAnalyzerParams]

  override var params: PatternMatchAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[PatternMatchAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcPatternMatch = {
    JdbcPatternMatch(params.column, params.pattern, params.where)
  }

  def analyzerWithSpark(): PatternMatch = {
    PatternMatch(params.column, params.pattern, params.where)
  }
}
