package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Mean
import com.amazon.deequ.analyzers.jdbc.JdbcMean
import com.amazon.deequ.backend.jobmanagement.{ColumnAndWhereAnalyzerParams, RequestParameter}
import org.json4s.JValue

object MeanAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  val name = "Mean"
  val description = " This is the average over all values of the given column. Only for numeric columns."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcMean = {
    JdbcMean(params.column, params.where)
  }

  def analyzerWithSpark(): Mean = {
    Mean(params.column, params.where)
  }
}
