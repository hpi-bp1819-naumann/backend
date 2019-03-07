package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Maximum
import com.amazon.deequ.analyzers.jdbc.JdbcMaximum
import com.amazon.deequ.backend.jobmanagement.{ColumnAndWhereAnalyzerParams, RequestParameter}
import org.json4s.JValue

object MaximumAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  val name = "Maximum"
  val description = "Returns the largest value in the given column. Only for numeric columns."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcMaximum = {
    JdbcMaximum(params.column, params.where)
  }

  def analyzerWithSpark(): Maximum = {
    Maximum(params.column, params.where)
  }
}
