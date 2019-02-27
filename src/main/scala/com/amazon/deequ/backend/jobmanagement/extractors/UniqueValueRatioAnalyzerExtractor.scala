package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.UniqueValueRatio
import com.amazon.deequ.analyzers.jdbc.JdbcUniqueValueRatio
import com.amazon.deequ.backend.jobmanagement.MultiColumnAnalyzerParams
import org.json4s.JValue

object UniqueValueRatioAnalyzerExtractor extends AnalyzerExtractor[MultiColumnAnalyzerParams] {
  override var params: MultiColumnAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[MultiColumnAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcUniqueValueRatio = {
    JdbcUniqueValueRatio(params.columns)
  }

  def analyzerWithSpark(): UniqueValueRatio = {
    UniqueValueRatio(params.columns)
  }
}
