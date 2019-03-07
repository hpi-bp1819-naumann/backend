package com.amazon.deequ.backend.jobmanagement.extractors

import com.amazon.deequ.analyzers.Entropy
import com.amazon.deequ.analyzers.jdbc.JdbcEntropy
import com.amazon.deequ.backend.jobmanagement.{ColumnAnalyzerParams, RequestParameter}
import org.json4s.JValue

object EntropyAnalyzerExtractor extends AnalyzerExtractor[ColumnAnalyzerParams] {
  val name = "Entropy"
  val description = "Entropy is a measure of the level of information contained in a message."

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[ColumnAnalyzerParams]

  override var params: ColumnAnalyzerParams = _

  def extractFromJson(requestParams: JValue): Unit = {
    params = requestParams.extract[ColumnAnalyzerParams]
  }

  def analyzerWithJdbc(): JdbcEntropy = {
    JdbcEntropy(params.column)
  }

  def analyzerWithSpark(): Entropy = {
    Entropy(params.column)
  }
}
