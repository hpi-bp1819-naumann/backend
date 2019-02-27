package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Distinctness
import com.amazon.deequ.analyzers.jdbc.JdbcDistinctness
import com.amazon.deequ.backend.jobmanagement.MultiColumnAnalyzerParams
import org.json4s.JValue

object DistinctnessAnalyzerExtractor extends AnalyzerExtractor[MultiColumnAnalyzerParams] {
  override var params: MultiColumnAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[MultiColumnAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcDistinctness = {
    JdbcDistinctness(params.columns)
  }

  def analyzerWithSpark(): Distinctness = {
    Distinctness(params.columns)
  }
}
