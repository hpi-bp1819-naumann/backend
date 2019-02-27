package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.StandardDeviation
import com.amazon.deequ.analyzers.jdbc.JdbcStandardDeviation
import com.amazon.deequ.backend.jobmanagement.ColumnAndWhereAnalyzerParams
import org.json4s.JValue

object StandardDeviationAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcStandardDeviation = {
    JdbcStandardDeviation(params.column, params.where)
  }

  def analyzerWithSpark(): StandardDeviation = {
    StandardDeviation(params.column, params.where)
  }
}
