package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Minimum
import com.amazon.deequ.analyzers.jdbc.JdbcMinimum
import com.amazon.deequ.backend.jobmanagement.{ColumnAndWhereAnalyzerParams, RequestParameter}
import org.json4s.JValue

object MinimumAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  val name = "Minimum"
  val description = "Returns the smallest value in the given column. Only for numeric columns."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

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
