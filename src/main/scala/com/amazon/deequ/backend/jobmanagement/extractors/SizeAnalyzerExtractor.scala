package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.analyzers.jdbc.JdbcSize
import com.amazon.deequ.backend.jobmanagement.{ColumnAndWhereAnalyzerParams, RequestParameter}
import org.json4s.JValue

object SizeAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  val name = "Size"
  val description = "Is the amount of values in the given column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcSize = {
    JdbcSize(params.where)
  }

  def analyzerWithSpark(): Size = {
    Size(params.where)
  }
}
