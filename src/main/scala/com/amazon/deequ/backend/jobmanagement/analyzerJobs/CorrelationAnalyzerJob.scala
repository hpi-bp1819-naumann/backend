package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import java.lang.reflect.Constructor

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{Correlation, CorrelationState}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, AnalyzerParams, ColumnAndWhereAnalyzerParams}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue

case class CorrelationAnalyzerParams(context: String, table: String,
                                var firstColumn: String, var secondColumn: String,
                                var where: Option[String] = None)
  extends AnalyzerParams

object CorrelationAnalyzerJob extends AnalyzerJob[CorrelationAnalyzerParams] {

  val name = "Correlation"
  val description = "description for correlation analyzer"

  val acceptedRequestParams: () => String = () => extractFieldNames[ColumnAndWhereAnalyzerParams]

  def extractFromJson(requestParams: JValue): CorrelationAnalyzerParams = {
    requestParams.extract[CorrelationAnalyzerParams]
  }

  def funcWithJdbc(params: CorrelationAnalyzerParams): Any = {
    analyzerWithJdbc[CorrelationState, DoubleMetric, JdbcCorrelation](
      JdbcCorrelation(params.firstColumn, params.secondColumn, params.where), params.table)
  }

  def funcWithSpark(params: CorrelationAnalyzerParams) {
    analyzerWithSpark[CorrelationState, DoubleMetric, Correlation](
      Correlation(params.firstColumn, params.secondColumn, params.where), params.table)
  }
}