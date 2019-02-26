package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{StandardDeviation, StandardDeviationState}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAndWhereAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object StandardDeviationAnalyzerJob extends AnalyzerJob[ColumnAndWhereAnalyzerParams] {

  val name = "StandardDeviation"
  val description = "Quantifies the amount of variation of the values in the given column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAndWhereAnalyzerParams = {
    requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAndWhereAnalyzerParams): Any = {
    analyzerWithJdbc[StandardDeviationState, DoubleMetric, JdbcStandardDeviation](
      JdbcStandardDeviation(params.column, params.where), params.table)
  }

  def funcWithSpark(params: ColumnAndWhereAnalyzerParams) {
    analyzerWithSpark[StandardDeviationState, DoubleMetric, StandardDeviation](
      StandardDeviation(params.column, params.where), params.table)
  }
}
