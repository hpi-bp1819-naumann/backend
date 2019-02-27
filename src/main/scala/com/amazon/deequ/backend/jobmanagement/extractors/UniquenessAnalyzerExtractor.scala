package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Uniqueness
import com.amazon.deequ.analyzers.jdbc.JdbcUniqueness
import com.amazon.deequ.backend.jobmanagement.{ColumnAndWhereAnalyzerParams, MultiColumnAnalyzerParams}
import org.json4s.JValue

object UniquenessAnalyzerExtractor extends AnalyzerExtractor[MultiColumnAnalyzerParams] {
  override var params: MultiColumnAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[MultiColumnAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcUniqueness = {
    JdbcUniqueness(params.columns)
  }

  def analyzerWithSpark(): Uniqueness = {
    Uniqueness(params.columns)
  }
}
