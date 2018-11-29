package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{DataType, DataTypeHistogram}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ColumnAndWhereAnalyzerParams}
import com.amazon.deequ.metrics.HistogramMetric
import org.json4s.JValue

object DataTypeAnalyzerJob extends AnalyzerJob[ColumnAndWhereAnalyzerParams] {

  val name = "DataType"
  val description = "description for dataType analyzer"

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