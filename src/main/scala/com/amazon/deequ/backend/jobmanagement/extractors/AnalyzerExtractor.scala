package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.jdbc.JdbcAnalyzer
import com.amazon.deequ.backend.jobmanagement.{AnalyzerParams, RequestParameter}
import com.amazon.deequ.metrics.Metric
import org.json4s.{DefaultFormats, Formats, JValue}

abstract class AnalyzerExtractor[T <: AnalyzerParams] {
  implicit val formats: Formats = DefaultFormats

  val name: String
  val description: String
  var params: T

  val acceptedRequestParams: () => Array[RequestParameter]

  def extractFieldNames[M <: Product:Manifest]: Array[RequestParameter] = {
    implicitly[Manifest[M]].erasure.getDeclaredFields.map(param =>
      RequestParameter(param.getName, param.getType.getSimpleName))
  }

  def extractFromJson(requestParams: JValue): Unit

  def analyzerWithJdbc(): JdbcAnalyzer[_, Metric[_]]
  def analyzerWithSpark(): Analyzer[_, Metric[_]]
}
