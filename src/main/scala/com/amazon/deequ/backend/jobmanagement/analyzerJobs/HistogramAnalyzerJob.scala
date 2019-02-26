package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{FrequenciesAndNumRows, Histogram}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.HistogramMetric
import org.json4s.JValue

object HistogramAnalyzerJob extends AnalyzerJob[ColumnAnalyzerParams] {

  val name = "Histogram"
  val description = "Divides the columns of the values in the given column into a given number of buckets and applies the given function to each bucket."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAnalyzerParams = {
    requestParams.extract[ColumnAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, HistogramMetric, JdbcHistogram](
      JdbcHistogram(params.column), params.table)
  }

  def funcWithSpark(params: ColumnAnalyzerParams) {
    analyzerWithSpark[FrequenciesAndNumRows, HistogramMetric, Histogram](
      Histogram(params.column), params.table)
  }
}
