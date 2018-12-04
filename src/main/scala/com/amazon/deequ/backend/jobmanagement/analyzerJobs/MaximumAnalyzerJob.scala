package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import java.lang.reflect.Constructor

import com.amazon.deequ.analyzers.jdbc.JdbcMaximum
import com.amazon.deequ.analyzers.{MaxState, Maximum}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAndWhereAnalyzerParams}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object MaximumAnalyzerJob extends AnalyzerJob[ColumnAndWhereAnalyzerParams] {

  val name = "Maximum"
  val description = "The Maximum analyzer calculates the maximum of a given column."

  val acceptedRequestParams: () => String = () => extractFieldNames[ColumnAndWhereAnalyzerParams]

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
