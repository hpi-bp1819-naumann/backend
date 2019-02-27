package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Histogram
import com.amazon.deequ.analyzers.jdbc.JdbcHistogram
import com.amazon.deequ.backend.jobmanagement.ColumnAnalyzerParams
import org.json4s.JValue

object HistogramAnalyzerExtractor extends AnalyzerExtractor[ColumnAnalyzerParams] {
  override var params: ColumnAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcHistogram = {
    JdbcHistogram(params.column)
  }

  def analyzerWithSpark(): Histogram = {
    Histogram(params.column)
  }
}
