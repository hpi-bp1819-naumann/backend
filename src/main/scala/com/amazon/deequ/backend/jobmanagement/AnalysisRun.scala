package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzer, Table}
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, JdbcAnalysisRunner}
import com.amazon.deequ.backend.jobmanagement.extractors._
import com.amazon.deequ.backend.utils.JdbcUtils.{connectionProperties, jdbcUrl, withJdbc, withSpark}
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

    val analysisRun = context match {
      case AnalyzerContext.jdbc =>
        val analyzers = parseJdbcAnalyzers(requestedAnalyzers)

        () => withJdbc[Seq[Metric[_]]] { connection =>
          val table = Table(tableName, connection)
          JdbcAnalysisRunner.doAnalysisRun(table, analyzers).allMetrics
        }
      case AnalyzerContext.spark =>
        val analyzers = parseSparkAnalyzers(requestedAnalyzers)

        () => withSpark { session =>
          val data = session.read.jdbc(jdbcUrl, tableName, connectionProperties())
          AnalysisRunner.doAnalysisRun(data, analyzers).allMetrics
        }
    }

    ExecutableAnalyzerJob("bla", analysisRun, body)
  }

  def parseAnalyzerExtractors(requestedAnalyzers: Seq[JValue]): Seq[AnalyzerExtractor[_]] = {
    requestedAnalyzers.map(analyzerParams => {
      val extractedParams = analyzerParams.extract[Map[String, Any]]
      println(extractedParams)
      val analyzerName = extractedParams("analyzer").toString
      println(analyzerName)

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

  def parseJdbcAnalyzers(requestedAnalyzers: Seq[JValue]): Seq[JdbcAnalyzer[_, Metric[_]]] = {
    val extractors = parseAnalyzerExtractors(requestedAnalyzers)
    extractors.map(extractor => extractor.analyzerWithJdbc())
  }

  def parseSparkAnalyzers(requestedAnalyzers: Seq[JValue]): Seq[Analyzer[_, Metric[_]]] = {
    val extractors = parseAnalyzerExtractors(requestedAnalyzers)
    extractors.map(extractor => extractor.analyzerWithSpark())
  }
}
