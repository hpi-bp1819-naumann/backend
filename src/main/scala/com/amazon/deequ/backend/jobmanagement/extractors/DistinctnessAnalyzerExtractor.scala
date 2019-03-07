package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Distinctness
import com.amazon.deequ.analyzers.jdbc.JdbcDistinctness
import com.amazon.deequ.backend.jobmanagement.{MultiColumnAnalyzerParams, RequestParameter}
import org.json4s.JValue

object DistinctnessAnalyzerExtractor extends AnalyzerExtractor[MultiColumnAnalyzerParams] {
  val name = "Distinctness"
  val description = "Distinctness is the fraction of the number of distinct values devided by the number of all values of a column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[MultiColumnAnalyzerParams]

  override var params: MultiColumnAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[MultiColumnAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcDistinctness = {
    JdbcDistinctness(params.columns)
  }

  def analyzerWithSpark(): Distinctness = {
    Distinctness(params.columns)
  }

  def parseQuery(tableName: String, params: MultiColumnAnalyzerParams): String = {
    val columns = params.columns
    val select = columns.mkString("", " , ", "")
    val where = columns.mkString("", " is not null and ", " is not null")

    s"""
       |SELECT
       | $select, count(*) as cnt
       |FROM
       | $tableName
       |WHERE
       | $where
       |GROUP BY
       | $select
    """.stripMargin
  }
}
