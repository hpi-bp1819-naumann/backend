package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Sum
import com.amazon.deequ.analyzers.jdbc.JdbcSum
import com.amazon.deequ.backend.jobmanagement.{ColumnAndWhereAnalyzerParams, RequestParameter}
import org.json4s.JValue

object SumAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  val name = "Sum"
  val description = "The sum of all values in the given column. Only for numeric columns."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcSum = {
    JdbcSum(params.column, params.where)
  }

  def analyzerWithSpark(): Sum = {
    Sum(params.column, params.where)
  }
}
