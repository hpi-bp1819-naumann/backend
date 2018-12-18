package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc.JdbcCompleteness
import com.amazon.deequ.analyzers.{Completeness, NumMatchesAndCount}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAndWhereAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object CompletenessAnalyzerJob extends AnalyzerJob[ColumnAndWhereAnalyzerParams] {

  val name = "Completeness"
  val description = "description for completeness analyzer"

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAndWhereAnalyzerParams = {
    requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAndWhereAnalyzerParams): Any = {
    analyzerWithJdbc[NumMatchesAndCount, DoubleMetric, JdbcCompleteness](
      JdbcCompleteness(params.column, params.where), params.table)
  }

  def funcWithSpark(params: ColumnAndWhereAnalyzerParams) {
    analyzerWithSpark[NumMatchesAndCount, DoubleMetric, Completeness](
      Completeness(params.column, params.where), params.table)
  }
}
