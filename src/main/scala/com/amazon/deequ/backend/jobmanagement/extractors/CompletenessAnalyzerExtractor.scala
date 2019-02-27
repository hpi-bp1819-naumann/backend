package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.analyzers.jdbc.JdbcCompleteness
import com.amazon.deequ.backend.jobmanagement.ColumnAndWhereAnalyzerParams
import org.json4s.JValue

object CompletenessAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcCompleteness = {
    JdbcCompleteness(params.column, params.where)
  }

  def analyzerWithSpark(): Completeness = {
    Completeness(params.column, params.where)
  }
}
