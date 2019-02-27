package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Sum
import com.amazon.deequ.analyzers.jdbc.JdbcSum
import com.amazon.deequ.backend.jobmanagement.ColumnAndWhereAnalyzerParams
import org.json4s.JValue

object SumAnalyzerExtractor extends AnalyzerExtractor[ColumnAndWhereAnalyzerParams] {
  override var params: ColumnAndWhereAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAndWhereAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcSum = {
    JdbcSum(params.column, params.where)
  }

  def analyzerWithSpark(): Sum = {
    Sum(params.column, params.where)
  }
}
