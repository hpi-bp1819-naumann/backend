package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{FrequenciesAndNumRows, Uniqueness}
import com.amazon.deequ.backend.jobmanagement._
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object UniquenessAnalyzerJob extends AnalyzerJob[MultiColumnAnalyzerParams] {

  val name = "Uniqueness"
  val description = "Gives the fraction of values of the given column that only appear once in the whole column divided by the Size of the column."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[MultiColumnAnalyzerParams]

  def extractFromJson(requestParams: JValue): MultiColumnAnalyzerParams = {
    requestParams.extract[MultiColumnAnalyzerParams]
  }

  def funcWithJdbc(params: MultiColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcUniqueness](
      JdbcUniqueness(params.columns), params.table)
  }

  def funcWithSpark(params: MultiColumnAnalyzerParams) {
    analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, Uniqueness](
      Uniqueness(params.columns), params.table)
  }

  def parseQuery(params: Map[String, Any]): String = {
    val tableName = params("table")
    val columns = params("columns").asInstanceOf[Seq[String]]
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