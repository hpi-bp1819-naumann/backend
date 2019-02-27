package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{CountDistinct, FrequenciesAndNumRows}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, MultiColumnAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object CountDistinctAnalyzerJob extends AnalyzerJob[MultiColumnAnalyzerParams] {

  val name = "CountDistinct"
  val description = "Number of distinct values in the column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[MultiColumnAnalyzerParams]

  def extractFromJson(requestParams: JValue): MultiColumnAnalyzerParams = {
    requestParams.extract[MultiColumnAnalyzerParams]
  }

  def funcWithJdbc(params: MultiColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcCountDistinct](
      JdbcCountDistinct(params.columns), params.table)
  }

  def funcWithSpark(params: MultiColumnAnalyzerParams): Any = {
    analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, CountDistinct](
      CountDistinct(params.columns), params.table)
  }
}