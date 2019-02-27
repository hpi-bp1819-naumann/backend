package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Minimum
import com.amazon.deequ.analyzers.jdbc.JdbcMinimum
import com.amazon.deequ.backend.jobmanagement.ColumnAndWhereAnalyzerParams
import org.json4s.JValue

object MinimumAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcMinimum = {
    JdbcMinimum(params.column, params.where)
  }

  def analyzerWithSpark(): Minimum = {
    Minimum(params.column, params.where)
  }
}
