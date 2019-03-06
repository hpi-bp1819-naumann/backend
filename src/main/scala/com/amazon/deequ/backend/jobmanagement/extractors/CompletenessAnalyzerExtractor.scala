package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Completeness
import com.amazon.deequ.analyzers.jdbc.JdbcCompleteness
import com.amazon.deequ.backend.jobmanagement.{ColumnAndWhereAnalyzerParams, RequestParameter}
import org.json4s.JValue

object CompletenessAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  val name = "Completeness"
  val description = "Completeness is the fraction of the number of non-null values divided by the number of all values in a column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcCompleteness = {
    JdbcCompleteness(params.column, params.where)
  }

  def analyzerWithSpark(): Completeness = {
    Completeness(params.column, params.where)
  }
}
