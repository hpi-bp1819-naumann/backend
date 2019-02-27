package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Mean
import com.amazon.deequ.analyzers.jdbc.JdbcMean
import com.amazon.deequ.backend.jobmanagement.ColumnAndWhereAnalyzerParams
import org.json4s.JValue

object MeanAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcMean = {
    JdbcMean(params.column, params.where)
  }

  def analyzerWithSpark(): Mean = {
    Mean(params.column, params.where)
  }
}
