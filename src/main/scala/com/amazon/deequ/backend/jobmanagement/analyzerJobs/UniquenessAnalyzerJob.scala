package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import java.lang.reflect.Constructor

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{FrequenciesAndNumRows, Uniqueness}
import com.amazon.deequ.backend.jobmanagement._
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object UniquenessAnalyzerJob extends AnalyzerJob[ColumnAnalyzerParams] {

  val name = "Uniqueness"
  val description = "description for uniqueness analyzer"

  val acceptedRequestParams: () => String = () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAnalyzerParams = {
    requestParams.extract[ColumnAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcUniqueness](
      JdbcUniqueness(params.column), params.table)
  }

  def funcWithSpark(params: ColumnAnalyzerParams) {
    analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, Uniqueness](
      Uniqueness(params.column), params.table)
  }
}