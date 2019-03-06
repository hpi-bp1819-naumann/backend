package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Uniqueness
import com.amazon.deequ.analyzers.jdbc.JdbcUniqueness
import com.amazon.deequ.backend.jobmanagement.{MultiColumnAnalyzerParams, RequestParameter}
import org.json4s.JValue

object UniquenessAnalyzerExtractor extends AnalyzerExtractor[MultiColumnAnalyzerParams] {
  val name = "Uniqueness"
  val description = "Gives the fraction of values of the given column that only appear once in the whole column divided by the Size of the column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[MultiColumnAnalyzerParams]

  override var params: MultiColumnAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[MultiColumnAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcUniqueness = {
    JdbcUniqueness(params.columns)
  }

  def analyzerWithSpark(): Uniqueness = {
    Uniqueness(params.columns)
  }

  def parseQuery(tableName: String, params: MultiColumnAnalyzerParams): String = {
    val columns = params.columns
    val select = columns.mkString("", " , ", "")
    val where = columns.mkString("", " is not null and ", " is not null")

    s"""
       |SELECT $select FROM (
       | SELECT
       |  $select, count(*) as cnt
       |	FROM
       |   $tableName
       |	WHERE
       |   $where
       |	GROUP BY
       |   $select
       | ) as GROUPING
       |WHERE cnt = 1
    """.stripMargin
  }
}
