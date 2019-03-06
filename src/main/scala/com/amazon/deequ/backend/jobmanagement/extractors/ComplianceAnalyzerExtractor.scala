package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Compliance
import com.amazon.deequ.analyzers.jdbc.JdbcCompliance
import com.amazon.deequ.backend.jobmanagement.RequestParameter
import com.amazon.deequ.backend.jobmanagement.analyzerJobs.ComplianceAnalyzerParams
import org.json4s.JValue

object ComplianceAnalyzerExtractor extends AnalyzerExtractor[ComplianceAnalyzerParams] {
  override var params: ComplianceAnalyzerParams = _
  val name = "Compliance"
  val description = "Compliance is a measure of the fraction of rows that complies with the given column constraint."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ComplianceAnalyzerParams]

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ComplianceAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcCompliance = {
    JdbcCompliance(params.instance, params.predicate, params.where)
  }

  def analyzerWithSpark(): Compliance = {
    Compliance(params.instance, params.predicate, params.where)
  }
}
