package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc.JdbcMaximum
import com.amazon.deequ.analyzers.{MaxState, Maximum}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAndWhereAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object MaximumAnalyzerJob extends AnalyzerJob[ColumnAndWhereAnalyzerParams] {

  val name = "Maximum"
  val description = "Returns the largest value in the given column. Only for numeric columns."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAndWhereAnalyzerParams = {
    requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAndWhereAnalyzerParams): Any = {
    analyzerWithJdbc[MaxState, DoubleMetric, JdbcMaximum](
      JdbcMaximum(params.column, params.where), params.table)
  }

  def funcWithSpark(params: ColumnAndWhereAnalyzerParams) {
    analyzerWithSpark[MaxState, DoubleMetric, Maximum](
      Maximum(params.column, params.where), params.table)
  }
}
