package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, MultiColumnAnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

object DistinctnessAnalyzerJob extends AnalyzerJob[MultiColumnAnalyzerParams] {

  val name = "Distinctness"
  val description = "description for distinctness analyzer"

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[MultiColumnAnalyzerParams]

  def extractFromJson(requestParams: JValue): MultiColumnAnalyzerParams = {
    requestParams.extract[MultiColumnAnalyzerParams]
  }

  def funcWithJdbc(params: MultiColumnAnalyzerParams): Any = {
    analyzerWithJdbc[JdbcFrequenciesAndNumRows, DoubleMetric, JdbcDistinctness](
      JdbcDistinctness(params.columns), params.table)
  }

  def funcWithSpark(params: MultiColumnAnalyzerParams) {
    analyzerWithSpark[FrequenciesAndNumRows, DoubleMetric, Distinctness](
      Distinctness(params.columns), params.table)
  }

  def parseQuery(params: Map[String, String]): String = {
    val tableName = params("table")
    // TODO: add multi column here
    val columns = Seq[String](params("column"))
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
