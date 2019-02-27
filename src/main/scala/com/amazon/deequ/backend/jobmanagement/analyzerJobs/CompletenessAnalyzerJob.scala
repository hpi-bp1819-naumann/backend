package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc.JdbcCompleteness
import com.amazon.deequ.analyzers.{Completeness, NumMatchesAndCount}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAndWhereAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object CompletenessAnalyzerJob extends AnalyzerJob[ColumnAndWhereAnalyzerParams] {

  val name = "Completeness"
  val description = "Completeness is the fraction of the number of non-null values devided by the number of all values in a column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAndWhereAnalyzerParams = {
    requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAndWhereAnalyzerParams): Any = {
    analyzerWithJdbc[NumMatchesAndCount, DoubleMetric, JdbcCompleteness](
      JdbcCompleteness(params.column, params.where), params.table)
  }

  def funcWithSpark(params: ColumnAndWhereAnalyzerParams): Any = {
    analyzerWithSpark[NumMatchesAndCount, DoubleMetric, Completeness](
      Completeness(params.column, params.where), params.table)
  }
}
