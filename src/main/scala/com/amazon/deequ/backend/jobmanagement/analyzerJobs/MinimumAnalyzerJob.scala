package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc.JdbcMinimum
import com.amazon.deequ.analyzers.{MinState, Minimum}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAndWhereAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object MinimumAnalyzerJob extends AnalyzerJob[ColumnAndWhereAnalyzerParams] {

  val name = "Minimum"
  val description = "The Minimum analyzer calculates the minimum of a given column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAndWhereAnalyzerParams = {
    requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAndWhereAnalyzerParams): Any = {
    analyzerWithJdbc[MinState, DoubleMetric, JdbcMinimum](
      JdbcMinimum(params.column, params.where), params.table)
  }

  def funcWithSpark(params: ColumnAndWhereAnalyzerParams) {
    analyzerWithSpark[MinState, DoubleMetric, Minimum](
      Minimum(params.column, params.where), params.table)
  }
}