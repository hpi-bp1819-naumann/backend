package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.DataType
import com.amazon.deequ.analyzers.jdbc.JdbcDataType
import com.amazon.deequ.backend.jobmanagement.{ColumnAndWhereAnalyzerParams, RequestParameter}
import org.json4s.JValue

object DataTypeAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  override var params: ColumnAndWhereAnalyzerParams = _
  val name = "DataType"
  val description = "Distribution map, including the overall number of values of each datatype and the percentage of each datatype."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcDataType = {
    JdbcDataType(params.column, params.where)
  }

  def analyzerWithSpark(): DataType = {
    DataType(params.column, params.where)
  }
}
