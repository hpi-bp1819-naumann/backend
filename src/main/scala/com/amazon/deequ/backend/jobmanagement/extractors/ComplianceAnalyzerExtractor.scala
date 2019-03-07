package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Compliance
import com.amazon.deequ.analyzers.jdbc.JdbcCompliance
import com.amazon.deequ.backend.jobmanagement.{AnalyzerParams, RequestParameter}
import org.json4s.JValue

case class ComplianceAnalyzerParams(analyzer: String,
                                    instance: String,
                                    predicate: String,
                                    where: Option[String] = None)
  extends AnalyzerParams {
  override def toMap: Map[String, Any] = {
    super.toMap ++ Map("instance" -> instance, "predicate" -> predicate, "where" -> where)
  }
}

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
