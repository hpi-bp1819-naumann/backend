package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.StandardDeviation
import com.amazon.deequ.analyzers.jdbc.JdbcStandardDeviation
import com.amazon.deequ.backend.jobmanagement.{ColumnAndWhereAnalyzerParams, RequestParameter}
import org.json4s.JValue

object StandardDeviationAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  val name = "StandardDeviation"
  val description = "Quantifies the amount of variation of the values in the given column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

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
