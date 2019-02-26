package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{DataType, DataTypeHistogram}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAndWhereAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.HistogramMetric
import org.json4s.JValue

object DataTypeAnalyzerJob extends AnalyzerJob[ColumnAndWhereAnalyzerParams] {

  val name = "DataType"
  val description = "Distribution map, including the overall number of values of each datatype and the percentage of each datatype."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  def extractFromJson(requestParams: JValue): ColumnAndWhereAnalyzerParams = {
    requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def funcWithJdbc(params: ColumnAndWhereAnalyzerParams): Any = {
    analyzerWithJdbc[DataTypeHistogram, HistogramMetric, JdbcDataType](
      JdbcDataType(params.column), params.table)
  }

  def funcWithSpark(params: ColumnAndWhereAnalyzerParams) {
    analyzerWithSpark[DataTypeHistogram, HistogramMetric, DataType](
      DataType(params.column), params.table)
  }
}