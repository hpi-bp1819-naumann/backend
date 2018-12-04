package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import java.lang.reflect.Constructor

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAnalyzerParams}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object DistinctnessAnalyzerJob extends AnalyzerJob[ColumnAnalyzerParams] {

  val name = "Distinctness"
  val description = "description for distinctness analyzer"

  val acceptedRequestParams: () => String = () => extractFieldNames[ColumnAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAnalyzerParams = {
    requestParams.extract[ColumnAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcDistinctness](
      JdbcDistinctness(params.column), params.table)
  }

  def funcWithSpark(params: ColumnAnalyzerParams) {
    analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, Distinctness](
      Distinctness(params.column), params.table)
  }
}
