package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{CountDistinct, FrequenciesAndNumRows}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAnalyzerParams}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object CountDistinctAnalyzerJob extends AnalyzerJob[ColumnAnalyzerParams] {

  val name = "CountDistinct"
  val description = "description for count distinct analyzer"

  def extractFromJson(requestParams: JValue): ColumnAnalyzerParams = {
    requestParams.extract[ColumnAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcCountDistinct](
      JdbcCountDistinct(params.column), params.table)
  }

  def funcWithSpark(params: ColumnAnalyzerParams) {
    analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, CountDistinct](
      CountDistinct(params.column), params.table)
  }
}