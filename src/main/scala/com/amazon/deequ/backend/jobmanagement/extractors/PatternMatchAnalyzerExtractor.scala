package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.PatternMatch
import com.amazon.deequ.analyzers.jdbc.JdbcPatternMatch
import com.amazon.deequ.backend.jobmanagement.AnalyzerParams
import org.json4s.JValue

import scala.util.matching.Regex

case class PatternMatchAnalyzerParams(context: String, table: String,
                                      var column: String, var pattern: Regex,
                                      var where: Option[String] = None)
  extends AnalyzerParams

object PatternMatchAnalyzerExtractor extends AnalyzerExtractor[PatternMatchAnalyzerParams] {
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
