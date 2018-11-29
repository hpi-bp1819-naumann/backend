package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{FrequenciesAndNumRows, Histogram}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAnalyzerParams}
import com.amazon.deequ.metrics.HistogramMetric
import org.json4s.JValue

object HistogramAnalyzerJob extends AnalyzerJob[ColumnAnalyzerParams] {

  val name = "Histogram"
  val description = "description for histogram analyzer"

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
