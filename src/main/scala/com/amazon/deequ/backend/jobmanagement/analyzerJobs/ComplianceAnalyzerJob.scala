package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import java.lang.reflect.Constructor

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{Compliance, NumMatchesAndCount}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, AnalyzerParams}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

case class ComplianceAnalyzerParams(context: String, table: String,
                               var instance: String, var predicate: String,
                               var where: Option[String] = None)
  extends AnalyzerParams

object ComplianceAnalyzerJob extends AnalyzerJob[ComplianceAnalyzerParams] {

  val name = "Compliance"
  val description = "description for compliance analyzer"

  val acceptedRequestParams: () => String = () => extractFieldNames[ComplianceAnalyzerParams]

  def extractFromJson(requestParams: JValue): ComplianceAnalyzerParams = {
    requestParams.extract[ComplianceAnalyzerParams]
  }

  def funcWithJdbc(params: ComplianceAnalyzerParams): Any = {
    analyzerWithJdbc[NumMatchesAndCount, DoubleMetric, JdbcCompliance](
      JdbcCompliance(params.instance, params.predicate, params.where), params.table)
  }

  def funcWithSpark(params: ComplianceAnalyzerParams): Unit = {
    analyzerWithSpark[NumMatchesAndCount, DoubleMetric, Compliance](
      Compliance(params.instance, params.predicate, params.where), params.table)
  }
}