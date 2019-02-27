package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.analyzers.jdbc.JdbcSize
import com.amazon.deequ.backend.jobmanagement.ColumnAndWhereAnalyzerParams
import org.json4s.JValue

object SizeAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
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
