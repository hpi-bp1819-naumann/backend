package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{FrequenciesAndNumRows, UniqueValueRatio}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, MultiColumnAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object UniqueValueRatioAnalyzerJob extends AnalyzerJob[MultiColumnAnalyzerParams] {

  val name = "UniqueValueRatio"
  val description = "The quotient of all unique values divided by all distinct values of the given column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[MultiColumnAnalyzerParams]

  def extractFromJson(requestParams: JValue): MultiColumnAnalyzerParams = {
    requestParams.extract[MultiColumnAnalyzerParams]
  }

  def funcWithJdbc(params: MultiColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcUniqueValueRatio](
      JdbcUniqueValueRatio(params.columns), params.table)
  }

  def funcWithSpark(params: MultiColumnAnalyzerParams): Any = {
    analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, UniqueValueRatio](
      UniqueValueRatio(params.columns), params.table)
  }
}