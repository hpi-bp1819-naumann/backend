package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.CountDistinct
import com.amazon.deequ.analyzers.jdbc.JdbcCountDistinct
import com.amazon.deequ.backend.jobmanagement.{MultiColumnAnalyzerParams, RequestParameter}
import org.json4s.JValue

object CountDistinctAnalyzerExtractor extends AnalyzerExtractor[MultiColumnAnalyzerParams] {
  override var params: MultiColumnAnalyzerParams = _
  val name = "CountDistinct"
  val description = "Number of distinct values in the column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[MultiColumnAnalyzerParams]

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[MultiColumnAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcCountDistinct = {
    JdbcCountDistinct(params.columns)
  }

  def analyzerWithSpark(): CountDistinct = {
    CountDistinct(params.columns)
  }
}
