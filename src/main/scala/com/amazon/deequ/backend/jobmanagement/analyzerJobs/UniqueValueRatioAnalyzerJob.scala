package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import java.lang.reflect.Constructor

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{FrequenciesAndNumRows, UniqueValueRatio}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAnalyzerParams}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object UniqueValueRatioAnalyzerJob extends AnalyzerJob[ColumnAnalyzerParams] {

  val name = "UniqueValueRatio"
  val description = "description for uniqueValueRatio analyzer"

  val acceptedRequestParams: () => String = () => extractFieldNames[ColumnAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAnalyzerParams = {
    requestParams.extract[ColumnAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcUniqueValueRatio](
      JdbcUniqueValueRatio(params.column), params.table)
  }

  def funcWithSpark(params: ColumnAnalyzerParams) {
    analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, UniqueValueRatio](
      UniqueValueRatio(params.column), params.table)
  }
}