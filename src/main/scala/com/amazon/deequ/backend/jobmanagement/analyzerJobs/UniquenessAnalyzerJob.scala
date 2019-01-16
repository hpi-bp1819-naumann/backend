package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{FrequenciesAndNumRows, Uniqueness}
import com.amazon.deequ.backend.jobmanagement._
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object UniquenessAnalyzerJob extends AnalyzerJob[ColumnAnalyzerParams] {

  val name = "Uniqueness"
  val description = "description for uniqueness analyzer"

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAndWhereAnalyzerParams]

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

  def parseQuery(params: Map[String, String]): String = {
    val tableName = params("table")
    // TODO: add multi column here
    val columns = Seq[String](params("column"))
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