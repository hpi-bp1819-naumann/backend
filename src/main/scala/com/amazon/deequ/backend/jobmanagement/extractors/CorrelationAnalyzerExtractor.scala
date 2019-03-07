package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Correlation
import com.amazon.deequ.analyzers.jdbc.JdbcCorrelation
import com.amazon.deequ.backend.jobmanagement.{AnalyzerParams, RequestParameter}
import org.json4s.JValue

case class CorrelationAnalyzerParams(analyzer: String,
                                     firstColumn: String,
                                     secondColumn: String,
                                     where: Option[String] = None)
  extends AnalyzerParams {
  override def toMap: Map[String, Any] = {
    super.toMap ++ Map("firstColumn" -> firstColumn, "secondColumn" -> secondColumn, "where" -> where)
  }
}

object CorrelationAnalyzerExtractor extends AnalyzerExtractor[CorrelationAnalyzerParams] {
  override var params: CorrelationAnalyzerParams = _
  val name = "Correlation"
  val description = "Pearson correlation coefficient between the two given columns"

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[CorrelationAnalyzerParams]

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[CorrelationAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcCorrelation = {
    JdbcCorrelation(params.firstColumn, params.secondColumn, params.where)
  }

  def analyzerWithSpark(): Correlation = {
    Correlation(params.firstColumn, params.secondColumn, params.where)
  }
}
