package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{Entropy, FrequenciesAndNumRows}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object EntropyAnalyzerJob extends AnalyzerJob[ColumnAnalyzerParams] {

  val name = "Entropy"
  val description = "Entropy is a measure of the level of information contained in a message."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAnalyzerParams = {
    requestParams.extract[ColumnAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcEntropy](
      JdbcEntropy(params.column), params.table)
  }

  def funcWithSpark(params: ColumnAnalyzerParams) {
    analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, Entropy](
      Entropy(params.column), params.table)
  }
}