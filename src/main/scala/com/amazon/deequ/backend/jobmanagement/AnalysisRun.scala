package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzer, Table}
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, JdbcAnalysisRunner}
import com.amazon.deequ.backend.DbSettings.DbSettings
import com.amazon.deequ.backend.jobmanagement.extractors._
import com.amazon.deequ.backend.utils.JdbcUtils.{connectionProperties, withJdbc, withSpark}
import com.amazon.deequ.metrics.Metric
import org.json4s.JsonAST.JValue
import org.json4s.{DefaultFormats, Formats}

import scala.collection.immutable.ListMap

case class AnalysisRun(tableName: String, context: String) {
  implicit val formats: Formats = DefaultFormats

  private val availableExtractors = ListMap[String, AnalyzerExtractor[_]](
    "completeness" -> CompletenessAnalyzerExtractor,
            "compliance" -> ComplianceAnalyzerExtractor,
            "correlation" -> CorrelationAnalyzerExtractor,
            "countDistinct" -> CountDistinctAnalyzerExtractor,
            "dataType" -> DataTypeAnalyzerExtractor,
            "distinctness" -> DistinctnessAnalyzerExtractor,
            "entropy" -> EntropyAnalyzerExtractor,
            "histogram" -> HistogramAnalyzerExtractor,
            "maximum" -> MaximumAnalyzerExtractor,
            "mean" -> MeanAnalyzerExtractor,
            "minimum" -> MinimumAnalyzerExtractor,
            "patternMatch" -> PatternMatchAnalyzerExtractor,
            "size" -> SizeAnalyzerExtractor,
            "standardDeviation" -> StandardDeviationAnalyzerExtractor,
            "sum" -> SumAnalyzerExtractor,
            "uniqueness" -> UniquenessAnalyzerExtractor,
            "uniqueValueRatio" -> UniqueValueRatioAnalyzerExtractor
  )

  def from(parsedBody: JValue): ExecutableAnalyzerJob = {

    val body = parsedBody.extract[Map[String, Seq[JValue]]]
    val requestedAnalyzers = body("analyzers")

    var params = Map[Any, AnalyzerParams]()

    val analysisRun = context match {
      case AnalyzerContext.jdbc =>
        val analyzerToParams = parseJdbcAnalyzers(requestedAnalyzers)
        params = analyzerToParams.asInstanceOf[Map[Any, AnalyzerParams]]
        val analyzers = analyzerToParams.keySet.toSeq

        () => withJdbc[Map[Any, Metric[_]]] { connection =>
          val table = Table(tableName, connection)
          JdbcAnalysisRunner.doAnalysisRun(table, analyzers).metricMap.asInstanceOf[Map[Any, Metric[_]]]
        }
      case AnalyzerContext.spark =>
        val analyzerToParams = parseSparkAnalyzers(requestedAnalyzers)
        params = analyzerToParams.asInstanceOf[Map[Any, AnalyzerParams]]
        val analyzers = analyzerToParams.keySet.toSeq

        () => withSpark[Map[Any, Metric[_]]] { session =>
          val data = session.read.jdbc(DbSettings.dburi, tableName, connectionProperties())
          AnalysisRunner.doAnalysisRun(data, analyzers).metricMap.asInstanceOf[Map[Any, Metric[_]]]
        }
    }

    ExecutableAnalyzerJob("AnalysisRun", tableName, context, analysisRun, params)
  }

  def parseAnalyzerExtractors(requestedAnalyzers: Seq[JValue]): Seq[AnalyzerExtractor[_]] = {
    requestedAnalyzers.map(analyzerParams => {
      val extractedParams = analyzerParams.extract[Map[String, Any]]
      val analyzerName = extractedParams("analyzer").toString

      if (!availableExtractors.exists(_._1 == analyzerName)) {
        throw new NoSuchAnalyzerException(
          s"There is no analyzer called $analyzerName. " +
            s"Available analyzers are ${availableExtractors.keys.mkString("[", ", ", "]")}")
      }

      val analyzerExtractor = availableExtractors(analyzerName)

      try {
        analyzerExtractor.extractFromJson(analyzerParams)
      } catch {
        case e: org.json4s.MappingException =>
          throw new RequestParamsException(
            s"There seems to be an error in your request parameters. " +
              s"The parameter extraction failed with: " + e.msg)
      }

      analyzerExtractor
    })
  }

  def parseJdbcAnalyzers(requestedAnalyzers: Seq[JValue]): Map[JdbcAnalyzer[_, Metric[_]], AnalyzerParams] = {
    val extractors = parseAnalyzerExtractors(requestedAnalyzers)
    extractors.map(extractor =>
      extractor.analyzerWithJdbc() -> extractor.params.asInstanceOf[AnalyzerParams]
    ).toMap
  }

  def parseSparkAnalyzers(requestedAnalyzers: Seq[JValue]): Map[Analyzer[_, Metric[_]], AnalyzerParams] = {
    val extractors = parseAnalyzerExtractors(requestedAnalyzers)
    extractors.map(extractor =>
      extractor.analyzerWithSpark() -> extractor.params.asInstanceOf[AnalyzerParams]
    ).toMap
  }
}
