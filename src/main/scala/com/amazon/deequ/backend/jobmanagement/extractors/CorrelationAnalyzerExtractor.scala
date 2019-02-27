package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Correlation
import com.amazon.deequ.analyzers.jdbc.JdbcCorrelation
import com.amazon.deequ.backend.jobmanagement.analyzerJobs.CorrelationAnalyzerParams
import org.json4s.JValue

object CorrelationAnalyzerExtractor extends AnalyzerExtractor[CorrelationAnalyzerParams] {
  override var params: CorrelationAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[CorrelationAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcCorrelation = {
    JdbcCorrelation(params.firstColumn, params.secondColumn, params.where)
  }

  def analyzerWithSpark(): Correlation = {
    Correlation(params.firstColumn, params.secondColumn, params.where)
  }
}
