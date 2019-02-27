package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzer
import com.amazon.deequ.backend.jobmanagement.AnalyzerParams
import com.amazon.deequ.metrics.Metric
import org.json4s.{DefaultFormats, Formats, JValue}

abstract class AnalyzerExtractor[T <: AnalyzerParams] {
  implicit val formats: Formats = DefaultFormats

  var params: T

  def extractFromJson(requestParams: JValue): Unit

  def analyzerWithJdbc(): JdbcAnalyzer[_, Metric[_]]
  def analyzerWithSpark(): Analyzer[_, Metric[_]]
}
