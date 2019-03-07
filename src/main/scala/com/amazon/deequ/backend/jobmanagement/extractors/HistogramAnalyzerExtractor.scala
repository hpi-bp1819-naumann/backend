package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Histogram
import com.amazon.deequ.analyzers.jdbc.JdbcHistogram
import com.amazon.deequ.backend.jobmanagement.{ColumnAnalyzerParams, RequestParameter}
import org.json4s.JValue

object HistogramAnalyzerExtractor extends AnalyzerExtractor[ColumnAnalyzerParams] {
  val name = "Histogram"
  val description = "Divides the columns of the values in the given column into a given number of buckets and applies the given function to each bucket."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAnalyzerParams]

  override var params: ColumnAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcHistogram = {
    JdbcHistogram(params.column)
  }

  def analyzerWithSpark(): Histogram = {
    Histogram(params.column)
  }
}
